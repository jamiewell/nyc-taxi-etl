"""
Year-month collection range resolution

There is no metadata table tracking what has already been processed, so a
requested --start-year-month/--end-year-month range is resolved against what
actually exists in the source layout at run time:

  <base_path>/<taxi_type>/year=YYYY/month=MM/...

(the nyc-taxi-collector S3 bucket layout). Partition listing uses Hadoop's
FileSystem API (via the Spark JVM gateway) rather than boto3, so this works
identically for local paths, s3a://, and HDFS without adding a new
dependency.
"""
import re

_YEAR_MONTH_RE = re.compile(r"^(\d{4})-(\d{1,2})$")
_PARTITION_RE = re.compile(r"year=(\d{4})/month=(\d{1,2})$")


def parse_year_month(value: str):
    match = _YEAR_MONTH_RE.match(value.strip())
    if not match:
        raise ValueError(f"Invalid year-month {value!r}. Expected format: YYYY-MM")
    year, month = int(match.group(1)), int(match.group(2))
    if not (1 <= month <= 12):
        raise ValueError(f"Invalid month in {value!r}: must be 01-12")
    return year, month


def _iter_year_months(start, end):
    year, month = start
    end_year, end_month = end
    result = []
    while (year, month) <= (end_year, end_month):
        result.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return result


def list_available_year_months(spark, base_path: str, taxi_type: str):
    """
    Lists (year, month) tuples that actually exist under
    <base_path>/<taxi_type>/year=*/month=*, via Hadoop's FileSystem API.
    """
    type_path = f"{base_path.rstrip('/')}/{taxi_type}"

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    glob_path = jvm.org.apache.hadoop.fs.Path(f"{type_path}/year=*/month=*")
    fs = glob_path.getFileSystem(hadoop_conf)

    try:
        statuses = fs.globStatus(glob_path)
    except Exception as e:
        raise ValueError(f"Failed to list partitions under {type_path}: {e}") from e

    year_months = []
    if statuses:
        for status in statuses:
            if not status.isDirectory():
                continue
            match = _PARTITION_RE.search(status.getPath().toString())
            if match:
                year_months.append((int(match.group(1)), int(match.group(2))))

    return sorted(set(year_months))


def resolve_year_month_paths(spark, base_path: str, taxi_type: str,
                              start_year_month: str = None, end_year_month: str = None):
    """
    Returns the list of <base_path>/<taxi_type>/year=Y/month=M paths that
    fall within [start_year_month, end_year_month] (inclusive; either bound
    may be omitted to mean "earliest/latest available") AND actually exist in
    the source. Months inside the requested range that don't exist are
    logged as warnings and skipped, not treated as errors - there's no
    metadata table to distinguish "not yet collected" from "genuinely
    doesn't exist", so we simply process what's there.
    """
    type_path = f"{base_path.rstrip('/')}/{taxi_type}"
    available = list_available_year_months(spark, base_path, taxi_type)

    if not available:
        raise ValueError(f"No data found under {type_path} (no year=*/month=* partitions)")

    start = parse_year_month(start_year_month) if start_year_month else available[0]
    end = parse_year_month(end_year_month) if end_year_month else available[-1]

    if start > end:
        raise ValueError(
            f"start-year-month {start[0]}-{start[1]:02d} is after "
            f"end-year-month {end[0]}-{end[1]:02d}"
        )

    requested = _iter_year_months(start, end)
    available_set = set(available)

    matched = [ym for ym in requested if ym in available_set]
    missing = [ym for ym in requested if ym not in available_set]

    if missing:
        missing_str = ", ".join(f"{y}-{m:02d}" for y, m in missing)
        print(f"WARNING [{taxi_type}]: requested but not found under {type_path}, skipped: {missing_str}")

    if not matched:
        raise ValueError(
            f"No data available for taxi_type={taxi_type} in range "
            f"{start[0]}-{start[1]:02d} ~ {end[0]}-{end[1]:02d} under {type_path}"
        )

    matched_str = ", ".join(f"{y}-{m:02d}" for y, m in matched)
    print(f"[{taxi_type}] processing {len(matched)} month(s): {matched_str}")

    return [f"{type_path}/year={y}/month={m:02d}" for y, m in matched]
