import json

from pywaybackup.db import Database, Index, and_, delete, func, or_, select, tuple_, update, waybackup_snapshots
from pywaybackup.files import CDXfile, CSVfile
from pywaybackup.Verbosity import Progressbar
from pywaybackup.Verbosity import Verbosity as vb
from pywaybackup.helper import url_split

func: callable


class SnapshotCollection:
    """
    Represents the interaction with the snapshot-collection contained in the snapshot database.
    """

    def __init__(self):
        self.db = Database()
        self.cdxfile = None
        self.csvfile = None
        self._mode_first = False
        self._mode_last = False
        self._max_snapshots_per_url = None
      
        self._cdx_total = 0  # absolute amount of snapshots in cdx file
        self._snapshot_total = 0  # absolute amount of snapshots in db

        self._snapshot_unhandled = 0  # all unhandled snapshots in the db (without response)
        self._snapshot_handled = 0  # snapshots with a response

        self._snapshot_faulty = 0  # error while parsing cdx line

        self._filter_duplicates = 0  # with identical url_archive
        self._filter_mode = 0  # all snapshots filtered by the MODE (last or first)
        self._filter_skip = 0  # content of the csv file
        self._filter_response = 0  # snapshots which could not be loaded from cdx file into db or 404

    def close(self):
        """
        Close up the collection, write result into csv, totals into db.
        """
        self._write_summary()
        self._reset_locked_snapshots()
        self._finalize_db()

    def _write_summary(self):
        """Write summary of download and skip counts."""
        success = self.count_success()
        fail = self.count_fail()
        vb.write(content=f"\n{'downloaded'.ljust(12)}: {success}")
        vb.write(content=f"{'skipped'.ljust(12)}: {fail}")

    def _reset_locked_snapshots(self):
        """Reset locked snapshots to unprocessed in the database."""
        self.db.session.execute(
            update(waybackup_snapshots).where(waybackup_snapshots.response == "LOCK").values(response=None)
        )
        self.db.session.commit()

    def _finalize_db(self):
        """Commit and close the database connection, and write progress."""
        self.db.write_progress(self._snapshot_handled, self._snapshot_total)
        self.db.session.close()

    def load(self, mode: str, cdxfile: CDXfile, csvfile: CSVfile, max_snapshots_per_url: int = None):
        """
        Insert the content of the cdx and csv file into the snapshot table.
        """
        self.cdxfile = cdxfile
        self.csvfile = csvfile
        if mode == "first":
            self._mode_first = True
        if mode == "last":
            self._mode_last = True

        line_count = self.cdxfile.count_rows()
        self._cdx_total = line_count
        if not self.db.get_insert_complete():
            vb.write(content="\ninserting snapshots...")
            self._insert_cdx()
            self.db.set_insert_complete()
        else:
            vb.write(verbose=True, content="\nAlready inserted CDX data into database")
        if not self.db.get_index_complete():
            vb.write(content="\nIndexing snapshots...")
            self._index_snapshots()  # create indexes for the snapshot table
            self.db.set_index_complete()
        else:
            vb.write(verbose=True, content="\nAlready indexed snapshots")
        if not self.db.get_filter_complete():
            vb.write(content="\nFiltering snapshots (last or first version)...")
            # set per-url limit (if provided) and then filter
            self._max_snapshots_per_url = max_snapshots_per_url
            self._filter_snapshots()  # filter: keep newest or oldest based on MODE
            self.db.set_filter_complete()
        else:
            vb.write(verbose=True, content="\nAlready filtered snapshots (last or first version)")

        self._skip_set()  # set response to NULL or read csv file and write values into db

        self._snapshot_unhandled = self.count_unhandled()  # count all unhandled in db
        self._snapshot_handled = self.count_handled()  # count all handled in db
        self._snapshot_total = self.count_total()  # count all in db

    def _insert_cdx(self):
        """
        Insert the content of the cdx file into the snapshot table.
        - Removes duplicates by url_archive (same timestamp and url_origin)
        - Filters the snapshots by the given mode (last or first)
        """

        def __parse_line(line):
            line = json.loads(line)
            line = {
                "timestamp": line[0],
                "digest": line[1],
                "mimetype": line[2],
                "statuscode": line[3],
                "origin": line[4],
            }
            url_archive = f"https://web.archive.org/web/{line['timestamp']}id_/{line['origin']}"
            statuscode = line["statuscode"] if line["statuscode"] in ("301", "404") else None
            return {
                "timestamp": line["timestamp"],
                "url_archive": url_archive,
                "url_origin": line["origin"],
                "response": statuscode,
            }

        def _insert_batch_safe(line_batch):
            # removes duplicates within the line_batch itself
            seen_keys = set()
            unique_batch = []
            for row in line_batch:
                key = (row["timestamp"], row["url_origin"], row["url_archive"])
                if key not in seen_keys:
                    seen_keys.add(key)
                    unique_batch.append(row)

            # removes duplicates from the line_batch if they are already in the database
            # get existing entries by tuple, remove existing rows from the unique_batch
            keys = [(row["timestamp"], row["url_origin"], row["url_archive"]) for row in unique_batch]
            existing = (
                self.db.session.query(
                    waybackup_snapshots.timestamp,
                    waybackup_snapshots.url_origin,
                    waybackup_snapshots.url_archive,
                )
                .filter(
                    tuple_(
                        waybackup_snapshots.timestamp, waybackup_snapshots.url_origin, waybackup_snapshots.url_archive
                    ).in_(keys)
                )
                .all()
            )
            existing_rows = set(existing)
            new_rows = [
                row
                for row in unique_batch
                if (row["timestamp"], row["url_origin"], row["url_archive"]) not in existing_rows
            ]
            if new_rows:
                self.db.session.bulk_insert_mappings(waybackup_snapshots, new_rows)
                self.db.session.commit()
            self._filter_duplicates += len(line_batch) - len(new_rows)
            return len(new_rows)

        vb.write(verbose=None, content="\nInserting CDX data into database...")

        try:
            vb.write(verbose=True, content="[SnapshotCollection._insert_cdx] starting insert_cdx operation")
            progressbar = Progressbar(
            unit=" lines",
            total=self._cdx_total,
            desc="process cdx".ljust(15),
            ascii="░▒█",
            bar_format="{l_bar}{bar:50}{r_bar}{bar:-10b}",
            )
            line_batchsize = 2500
            line_batch = []
            total_inserted = 0
            first_line = True

            with self.cdxfile as f:
                for line in f:
                    if first_line:
                        first_line = False
                        continue
                    line = line.strip()
                    if line.endswith("]]"):
                        line = line.rsplit("]", 1)[0]
                    if line.endswith(","):
                        line = line.rsplit(",", 1)[0]

                    try:
                        line_batch.append(__parse_line(line))
                    except json.decoder.JSONDecodeError:
                        self._snapshot_faulty += 1
                        continue

                    if len(line_batch) >= line_batchsize:
                        total_inserted += _insert_batch_safe(line_batch=line_batch)
                        line_batch = []
                        progressbar.update(line_batchsize)

                if line_batch:
                    total_inserted += _insert_batch_safe(line_batch=line_batch)
                    progressbar.update(len(line_batch))

            self.db.session.commit()
            vb.write(verbose=True, content="[SnapshotCollection._insert_cdx] insert_cdx commit successful")
        except Exception as e:
            vb.write(verbose=True, content=f"[SnapshotCollection._insert_cdx] exception: {e}; rolling back")
            try:
                self.db.session.rollback()
                vb.write(verbose=True, content="[SnapshotCollection._insert_cdx] rollback successful")
            except Exception:
                vb.write(verbose=True, content="[SnapshotCollection._insert_cdx] rollback failed")
            raise

    def _index_snapshots(self):
        """
        Create indexes for the snapshot table.
        """
        # index for filtering last snapshots
        if self._mode_last:
            idx1 = Index(
                "idx_waybackup_snapshots_url_origin_timestamp_desc",
                waybackup_snapshots.url_origin,
                waybackup_snapshots.timestamp.desc(),
            )
            idx1.create(self.db.session.bind, checkfirst=True)
        # index for filtering first snapshots
        if self._mode_first:
            idx2 = Index(
                "idx_waybackup_snapshots_url_origin_timestamp_asc",
                waybackup_snapshots.url_origin,
                waybackup_snapshots.timestamp.asc(),
            )
            idx2.create(self.db.session.bind, checkfirst=True)
        # index for skippable snapshots
        idx3 = Index(
            "idx_waybackup_snapshots_timestamp_url_origin_response",
            waybackup_snapshots.timestamp,
            waybackup_snapshots.url_origin,
        )
        idx3.create(self.db.session.bind, checkfirst=True)

    def _filter_snapshots(self):
        """
        Filter the snapshot table by applying mode and per-URL limits.
        Orchestrates the full filtering pipeline.
        """
        self._filter_mode_snapshots()
        self._filter_by_max_snapshots_per_url()
        self._enumerate_counter()
        self._count_response_status()

    def _filter_mode_snapshots(self):
        """
        Filter snapshots by mode (first or last version).
        
        - MODE_LAST: keep only the latest snapshot (highest timestamp) per url_origin.
        - MODE_FIRST: keep only the earliest snapshot (lowest timestamp) per url_origin.
        """
        self._filter_mode = 0
        if self._mode_last or self._mode_first:
            ordering = (
                waybackup_snapshots.timestamp.desc() if self._mode_last else waybackup_snapshots.timestamp.asc()
            )
            # assign row numbers per url_origin
            rownum = (
                func.row_number()
                .over(
                    partition_by=waybackup_snapshots.url_origin,
                    order_by=ordering,
                )
                .label("rn")
            )
            subq = select(waybackup_snapshots.scid, rownum).subquery()
            # keep rn == 1, delete all others
            keepers = select(subq.c.scid).where(subq.c.rn == 1)
            stmt = delete(waybackup_snapshots).where(~waybackup_snapshots.scid.in_(keepers))
            result = self.db.session.execute(stmt)
            self.db.session.commit()
            self._filter_mode = result.rowcount

    def _filter_by_max_snapshots_per_url(self):
        """
        Limit snapshots per unique URL, distributed across the date range.
        
        Keeps up to `self._max_snapshots_per_url` snapshots per `url_origin`.
        Selection is distributed across available timestamps to represent the full timespan.
        First and last snapshots are preserved when limit > 1.
        """
        self._filter_snapshot_deleted = 0
        limit = self._parse_snapshot_limit()
        
        if not (limit and limit > 0):
            return

        origins = self._find_origins_exceeding_limit(limit)
        
        for origin_row in origins:
            origin = origin_row[0]
            deleted_count = self._apply_limit_to_origin(origin, limit)
            self._filter_snapshot_deleted += deleted_count

    def _parse_snapshot_limit(self) -> int:
        """
        Parse and validate the max_snapshots_per_url configuration.
        
        Returns:
            The validated limit as an integer, or None if invalid/not set.
        """
        limit = None
        if self._max_snapshots_per_url:
            try:
                limit = int(self._max_snapshots_per_url)
            except (TypeError, ValueError):
                limit = None
        return limit

    def _find_origins_exceeding_limit(self, limit: int) -> list:
        """
        Query the database for origins that have more snapshots than the limit.
        
        Args:
            limit: Maximum number of snapshots allowed per origin.
            
        Returns:
            List of origin rows that exceed the limit.
        """
        origins = (
            self.db.session.execute(
                select(waybackup_snapshots.url_origin, func.count().label("cnt"))
                .group_by(waybackup_snapshots.url_origin)
                .having(func.count() > limit)
            )
            .all()
        )
        return origins

    def _apply_limit_to_origin(self, origin: str, limit: int) -> int:
        """
        Apply the snapshot limit to a specific origin by deleting excess snapshots.
        
        Fetches all snapshots for the origin, computes which ones to keep using
        distributed selection, and deletes the rest.
        
        Args:
            origin: The url_origin to apply the limit to.
            limit: Maximum number of snapshots to keep.
            
        Returns:
            Number of snapshots deleted for this origin.
        """
        scid_rows = self._fetch_ordered_scids(origin)
        total = len(scid_rows)
        
        if total <= limit:
            return 0

        indices = self._compute_distributed_indices(total, limit)
        keep_scids = [scid_rows[i] for i in indices]
        
        deleted_count = self._delete_excess_snapshots(origin, keep_scids)
        return deleted_count

    def _fetch_ordered_scids(self, origin: str) -> list:
        """
        Fetch all snapshot IDs for a given origin, ordered by timestamp ascending.
        
        Args:
            origin: The url_origin to fetch snapshots for.
            
        Returns:
            List of scid values in timestamp order.
        """
        scid_rows = (
            self.db.session.execute(
                select(waybackup_snapshots.scid)
                .where(waybackup_snapshots.url_origin == origin)
                .order_by(waybackup_snapshots.timestamp.asc())
            )
            .scalars()
            .all()
        )
        return scid_rows

    def _delete_excess_snapshots(self, origin: str, keep_scids: list) -> int:
        """
        Delete snapshots for an origin that are not in the keep list.
        
        Args:
            origin: The url_origin to delete snapshots from.
            keep_scids: List of scid values to preserve.
            
        Returns:
            Number of snapshots deleted.
        """
        stmt = delete(waybackup_snapshots).where(
            and_(waybackup_snapshots.url_origin == origin, ~waybackup_snapshots.scid.in_(keep_scids))
        )
        result = self.db.session.execute(stmt)
        self.db.session.commit()
        return result.rowcount

    def _compute_distributed_indices(self, total: int, limit: int) -> list:
        """
        Compute which indices to keep for time-distributed snapshot selection.
        
        Ensures snapshots are evenly distributed across the date range,
        preserving first and last snapshots when limit > 1.
        
        Args:
            total: Total number of snapshots available.
            limit: Maximum number of snapshots to keep.
            
        Returns:
            Sorted list of indices to keep.
        """
        indices = []
        if limit == 1:
            # Pick middle snapshot as representative
            indices = [total // 2]
        else:
            for i in range(limit):
                idx = round(i * (total - 1) / (limit - 1))
                indices.append(int(idx))

        # Ensure unique and valid indices
        indices = sorted(set(max(0, min(total - 1, i)) for i in indices))
        return indices

    def _enumerate_counter(self):
        """
        Assign sequential counter values to all snapshots.
        
        Sets the counter field (snapshot number x / y) for ordering and progress tracking.
        Processes in batches for efficiency.
        """
        offset = 1
        batch_size = 5000
        while True:
            rows = (
                self.db.session.execute(
                    select(waybackup_snapshots.scid)
                    .where(waybackup_snapshots.counter.is_(None))
                    .order_by(waybackup_snapshots.scid)
                    .limit(batch_size)
                )
                .scalars()
                .all()
            )
            if not rows:
                break
            mappings = [{"scid": scid, "counter": i} for i, scid in enumerate(rows, start=offset)]
            self.db.session.bulk_update_mappings(waybackup_snapshots, mappings)
            self.db.session.commit()
            offset += len(rows)

    def _count_response_status(self):
        """Count snapshots with non-retrieval status codes (404, 301)."""
        self._filter_response = (
            self.db.session.query(waybackup_snapshots).where(waybackup_snapshots.response.in_(["404", "301"])).count()
        )
        self.db.session.commit()

    def _skip_set(self):
        """
        If an existing csv-file for the job was found, the responses will be overwritten by the csv-content.
        """

        # ? for now per row / no bulk for compatibility
        try:
            vb.write(verbose=True, content="[SnapshotCollection._skip_set] applying CSV skips to DB")
            with self.csvfile as f:
                total_skipped = 0
                for row in f:
                    self.db.session.execute(
                        update(waybackup_snapshots)
                        .where(
                            and_(
                                waybackup_snapshots.timestamp == row["timestamp"],
                                waybackup_snapshots.url_origin == row["url_origin"],
                            )
                        )
                        .values(
                            url_archive=row["url_archive"],
                            redirect_url=row["redirect_url"],
                            redirect_timestamp=row["redirect_timestamp"],
                            response=row["response"],
                            file=row["file"],
                        )
                    )
                    total_skipped += 1

            self.db.session.commit()
            self._filter_skip = total_skipped
            vb.write(verbose=True, content=f"[SnapshotCollection._skip_set] commit successful, total_skipped={total_skipped}")
        except Exception as e:
            vb.write(verbose=True, content=f"[SnapshotCollection._skip_set] exception: {e}; rolling back")
            try:
                self.db.session.rollback()
                vb.write(verbose=True, content="[SnapshotCollection._skip_set] rollback successful")
            except Exception:
                vb.write(verbose=True, content="[SnapshotCollection._skip_set] rollback failed")
            raise

    def count_total(self) -> int:
        return self.db.session.query(waybackup_snapshots.scid).count()

    def count_handled(self) -> int:
        return self.db.session.query(waybackup_snapshots.scid).where(waybackup_snapshots.response.is_not(None)).count()

    def count_unhandled(self) -> int:
        return self.db.session.query(waybackup_snapshots.scid).where(waybackup_snapshots.response.is_(None)).count()

    def count_success(self) -> int:
        return (
            self.db.session.query(waybackup_snapshots.scid)
            .where(and_(waybackup_snapshots.file.is_not(None), waybackup_snapshots.file != ""))
            .count()
        )

    def count_fail(self) -> int:
        return (
            self.db.session.query(waybackup_snapshots.scid)
            .where(or_(waybackup_snapshots.file.is_(None), waybackup_snapshots.file == ""))
            .count()
        )

    def print_calculation(self):
        vb.write(content="\nSnapshot calculation:")
        vb.write(content=f"-----> {'in CDX file'.ljust(18)}: {self._cdx_total:,}")

        if self._filter_duplicates > 0:
            vb.write(content=f"-----> {'removed duplicates'.ljust(18)}: {self._filter_duplicates:,}")
        if self._filter_mode > 0:
            vb.write(content=f"-----> {'removed versions'.ljust(18)}: {self._filter_mode:,}")

        if self._filter_skip > 0:
            vb.write(content=f"-----> {'skip existing'.ljust(18)}: {self._filter_skip:,}")
        if self._filter_response > 0:
            vb.write(content=f"-----> {'skip statuscode'.ljust(18)}: {self._filter_response}")

        if self._snapshot_unhandled > 0:
            vb.write(content=f"\n-----> {'to utilize'.ljust(18)}: {self._snapshot_unhandled:,}")
        else:
            vb.write(content=f"-----> {'unhandled'.ljust(18)}: {self._snapshot_unhandled:,}")

        if self._snapshot_faulty > 0:
            vb.write(content=f"\n-----> {'!!! parsing error'.ljust(18)}: {self._snapshot_faulty:,}")
