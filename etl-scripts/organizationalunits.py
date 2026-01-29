import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

# ---------- schemas ----------
org_units_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("Organization", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Code", StringType(), True),
    StructField("StartDate", TimestampType(), True),
    StructField("EndDate", TimestampType(), True),
    StructField("IsActive", BooleanType(), True),
    StructField("CreatedDate", TimestampType(), True),
    StructField("IsDeleted", BooleanType(), True),
    StructField("DeletedDate", TimestampType(), True),
    StructField("RecycledDate", TimestampType(), True),
    StructField("Version", LongType(), True),
    StructField("OrgUnitTypeId", IntegerType(), True),
])

parents_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("ParentOrgUnitId", IntegerType(), False),
    StructField("RowVersion", LongType(), True),
    StructField("DateDeleted", TimestampType(), True),
])

ancestors_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("AncestorOrgUnitId", IntegerType(), False),
])

desc_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("DescendantOrgUnitId", IntegerType(), False),
])

recent_access_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("UserId", IntegerType(), False),  # do NOT publish raw
    StructField("LastAccessedDate", TimestampType(), True),
])

# ---------- helpers ----------
def read_csv(path: str, schema: StructType):
    return (spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(path)
    )

def norm_ws(col):
    # trim + collapse internal whitespace
    return F.trim(F.regexp_replace(col, r"\s+", " "))

def format_ts_for_csv(df, ts_cols):
    # publish timestamps as ISO-like strings (UTC-ish). adjust if you need strict ISO 8601 with 'Z'
    out = df
    for c in ts_cols:
        if c in out.columns:
            out = out.withColumn(c, F.date_format(F.col(c), "yyyy-MM-dd'T'HH:mm:ss"))
    return out

def write_csv_publish(df, name: str, single_file: bool = False):
    out = df
    if single_file:
        out = out.coalesce(1)

    (out.write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .option("quoteAll", "true")
        .option("escape", "\"")
        .option("emptyValue", "")
        .option("nullValue", "")
        .save(f"{OUT_BASE}/{name}")
    )

# Choose once for your publishing needs:
SINGLE_FILE_PER_TABLE = True

def main(raw_base: str, out_base: str):
    global RAW_BASE, OUT_BASE, spark

    RAW_BASE = raw_base
    OUT_BASE = out_base

    spark = (
        SparkSession.builder
        .appName("Org Units ETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    org_units_raw = read_csv(f"{RAW_BASE}/OrganizationalUnits/OrganizationalUnits.csv", org_units_schema)
    parents_raw   = read_csv(f"{RAW_BASE}/OrganizationalUnits/OrganizationalUnitParents.csv", parents_schema)
    anc_raw       = read_csv(f"{RAW_BASE}/OrganizationalUnits/OrganizationalUnitAncestors.csv", ancestors_schema)
    desc_raw      = read_csv(f"{RAW_BASE}/OrganizationalUnits/OrganizationalUnitDescendants.csv", desc_schema)

    # Optional (PII-ish): only use to create aggregates
    recent_raw    = read_csv(f"{RAW_BASE}/OrganizationalUnits/OrganizationalUnitRecentAccess.csv", recent_access_schema)

    # Task 1) OrganizationalUnits “latest per OrgUnitId” (dedupe)
    w = Window.partitionBy("OrgUnitId").orderBy(
        F.col("Version").desc_nulls_last(),
        F.col("CreatedDate").desc_nulls_last(),
    )

    org_units_latest = (
        org_units_raw
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .drop("rn")
    )

    write_csv_publish(
        format_ts_for_csv(
            org_units_latest,
            [
                "StartDate",
                "EndDate",
                "CreatedDate",
                "DeletedDate",
                "RecycledDate",
            ],
        ),
        "org_units_latest_rawtyped",
        single_file=SINGLE_FILE_PER_TABLE,
    )

    # Task 2) Standardize text columns (publish-friendly)

    org_units_std = (org_units_latest
    .withColumn("Organization_std", norm_ws(F.col("Organization")))
    .withColumn("Type_std",         norm_ws(F.col("Type")))
    .withColumn("Name_std",         norm_ws(F.col("Name")))
    .withColumn("Code_std",         norm_ws(F.col("Code")))
    .withColumn("Name_norm",        F.upper(F.col("Name_std")))
    .withColumn("Code_norm",        F.upper(F.col("Code_std")))
    )

    # publish a clean, stable column set
    org_units_clean = (org_units_std
        .select(
            "OrgUnitId",
            "OrgUnitTypeId",
            F.col("Type_std").alias("Type"),
            F.col("Organization_std").alias("Organization"),
            F.col("Name_std").alias("Name"),
            F.col("Code_std").alias("Code"),
            "StartDate","EndDate",
            "IsActive","IsDeleted",
            "CreatedDate","DeletedDate","RecycledDate",
            "Version",
            "Name_norm","Code_norm"
        )
    )

    write_csv_publish(format_ts_for_csv(org_units_clean, ["StartDate","EndDate","CreatedDate","DeletedDate","RecycledDate"]),
                    "org_units_clean",
                    single_file=SINGLE_FILE_PER_TABLE)


    # Task 3) Lifecycle derivations (available now + lifecycle_state)

    now_utc = F.current_timestamp()

    org_units_lifecycle = (org_units_clean
        .withColumn(
            "is_within_dates",
            (F.col("StartDate").isNull() | (F.col("StartDate") <= now_utc)) &
            (F.col("EndDate").isNull()   | (F.col("EndDate")   >= now_utc))
        )
        .withColumn(
            "is_available_now",
            (F.coalesce(F.col("IsActive"), F.lit(False)) == True) &
            (F.coalesce(F.col("IsDeleted"), F.lit(False)) == False) &
            (F.col("is_within_dates") == True)
        )
        .withColumn(
            "lifecycle_state",
            F.when(F.col("IsDeleted") == True, F.lit("deleted_or_recycled"))
            .when(F.col("EndDate").isNotNull() & (F.col("EndDate") < now_utc), F.lit("ended"))
            .when(F.col("StartDate").isNotNull() & (F.col("StartDate") > now_utc), F.lit("future"))
            .when(F.col("IsActive") == True, F.lit("active"))
            .otherwise(F.lit("inactive"))
        )
    )

    write_csv_publish(format_ts_for_csv(org_units_lifecycle, ["StartDate","EndDate","CreatedDate","DeletedDate","RecycledDate"]),
                    "org_units_lifecycle",
                    single_file=SINGLE_FILE_PER_TABLE)

    # Task 4) Type dimension (OrgUnitTypeId → Type)

    dim_org_unit_type = (org_units_lifecycle
        .select("OrgUnitTypeId", "Type")
        .where(F.col("OrgUnitTypeId").isNotNull())
        .dropDuplicates(["OrgUnitTypeId", "Type"])
    )

    write_csv_publish(dim_org_unit_type, "dim_org_unit_type", single_file=SINGLE_FILE_PER_TABLE)
    # qc_type_conflicts = (dim_org_unit_type
    #     .groupBy("OrgUnitTypeId")
    #     .agg(F.countDistinct("Type").alias("type_name_variants"))
    #     .where(F.col("type_name_variants") > 1)
    # )

    # write_csv_publish(qc_type_conflicts, "qc_type_id_conflicts", single_file=SINGLE_FILE_PER_TABLE)

    # Task 5) Parents “current active edge” + full parents table

    # Full cleaned parents (dedupe)
    w = Window.partitionBy("OrgUnitId").orderBy(F.col("RowVersion").desc_nulls_last())
    parents_latest = (parents_raw
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .drop("rn")
    )

    parents_all_clean = parents_raw.dropDuplicates(["OrgUnitId","ParentOrgUnitId","RowVersion","DateDeleted"])
    parents_active = parents_latest.where(F.col("DateDeleted").isNull())

    write_csv_publish(format_ts_for_csv(parents_all_clean, ["DateDeleted"]),
                    "org_unit_parents_all",
                    single_file=SINGLE_FILE_PER_TABLE)

    write_csv_publish(parents_active, "org_unit_parents_active", single_file=SINGLE_FILE_PER_TABLE)

    # Task 6) Ancestors + Descendants cleaned (dedupe)

    anc_clean  = anc_raw.dropDuplicates(["OrgUnitId","AncestorOrgUnitId"])
    desc_clean = desc_raw.dropDuplicates(["OrgUnitId","DescendantOrgUnitId"])

    write_csv_publish(anc_clean,  "org_unit_ancestors",   single_file=SINGLE_FILE_PER_TABLE)
    write_csv_publish(desc_clean, "org_unit_descendants", single_file=SINGLE_FILE_PER_TABLE)


    # Task 7) Tree/path snapshot (publishable strings, not arrays)

    MAX_DEPTH = 12

    nodes = (org_units_lifecycle
        .select(
            F.col("OrgUnitId").alias("node_id"),
            F.col("Name").alias("node_name"),
            "OrgUnitTypeId", "Type",
            "is_available_now", "lifecycle_state"
        )
    )

    edges = (parents_active
        .select(
            F.col("OrgUnitId").alias("child_id"),
            F.col("ParentOrgUnitId").alias("parent_id")
        )
    )

    tree = (nodes
        .join(edges, nodes.node_id == edges.child_id, "left")
        .drop("child_id")
        .withColumn("path_ids",   F.array(F.col("node_id")))
        .withColumn("path_names", F.array(F.col("node_name")))
        .withColumn("depth",      F.lit(0))
        .withColumn("cur_parent", F.col("parent_id"))
    )

    parents_lookup = edges.select(F.col("child_id").alias("p_child"), F.col("parent_id").alias("p_parent"))

    for _ in range(MAX_DEPTH):
        p = (nodes
            .select(F.col("node_id").alias("p_id"), F.col("node_name").alias("p_name"))
            .join(parents_lookup, F.col("p_id") == F.col("p_child"), "left")
            .select("p_id", "p_name", "p_parent")
        )

        tree = (tree
            .join(p, tree.cur_parent == p.p_id, "left")
            .withColumn("path_ids",   F.when(p.p_id.isNotNull(),   F.concat(tree.path_ids,   F.array(p.p_id))).otherwise(tree.path_ids))
            .withColumn("path_names", F.when(p.p_name.isNotNull(), F.concat(tree.path_names, F.array(p.p_name))).otherwise(tree.path_names))
            .withColumn("depth",      F.when(p.p_id.isNotNull(), tree.depth + 1).otherwise(tree.depth))
            .withColumn("cur_parent", p.p_parent)
            .drop("p_id", "p_name", "p_parent")
        )

    tree_snapshot = (tree
        .withColumn("root_org_unit_id", F.element_at(F.col("path_ids"), -1))
        .withColumn("path_ids_str",     F.concat_ws(">", F.col("path_ids").cast("array<string>")))
        .withColumn("path_names_str",   F.concat_ws(" > ", F.col("path_names")))
        .drop("path_ids", "path_names", "cur_parent", "parent_id")
        .select(
            F.col("node_id").alias("OrgUnitId"),
            "OrgUnitTypeId", "Type", "node_name",
            "depth", "root_org_unit_id",
            "path_ids_str", "path_names_str",
            "is_available_now", "lifecycle_state"
        )
    )

    write_csv_publish(tree_snapshot, "org_unit_tree_snapshot", single_file=SINGLE_FILE_PER_TABLE)

    # Task 8) Rollups: descendant counts by type (great for publishing)

    desc_pairs = desc_clean.select(
        F.col("OrgUnitId").alias("ancestor_id"),
        F.col("DescendantOrgUnitId").alias("desc_id")
    )

    desc_units = (org_units_lifecycle
        .select(
            F.col("OrgUnitId").alias("desc_id"),
            "OrgUnitTypeId", "Type", "is_available_now"
        )
    )

    rollup_desc_by_type = (desc_pairs
        .join(desc_units, "desc_id", "left")
        .groupBy("ancestor_id", "OrgUnitTypeId", "Type")
        .agg(
            F.count("*").alias("desc_total"),
            F.sum(F.when(F.col("is_available_now") == True, 1).otherwise(0)).alias("desc_available_now")
        )
        .withColumnRenamed("ancestor_id", "OrgUnitId")
    )

    write_csv_publish(rollup_desc_by_type, "rollup_descendants_by_type", single_file=SINGLE_FILE_PER_TABLE)

    # Optional low-PII “usage” aggregates (publish OK, no user rows)

    org_unit_last_access = (recent_raw
        .groupBy("OrgUnitId")
        .agg(F.max("LastAccessedDate").alias("last_accessed"))
    )

    write_csv_publish(format_ts_for_csv(org_unit_last_access, ["last_accessed"]),
                    "org_unit_last_accessed",
                    single_file=SINGLE_FILE_PER_TABLE)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: test.py <raw_base> <out_base>")
        sys.exit(1)

    raw_base = sys.argv[1]
    out_base = sys.argv[2]
    main(raw_base, out_base)

