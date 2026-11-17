"""
Hive Analytics Query Runner.
Executes analytical SQL queries against the `india_logistics_dw` data warehouse,
validates results, and exports query execution summaries.
"""

import sys
import time
import re
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import HIVE_DATABASE_NAME
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session
from hive.init_hive_warehouse import init_hive_schema

logger = get_logger("HiveQueryRunner")

def parse_sql_file(sql_file_path: Path):
    """
    Parses a SQL script into individual executable queries,
    extracting query comments as descriptive titles.
    """
    with open(sql_file_path, "r", encoding="utf-8") as f:
        content = f.read()

    raw_chunks = content.split(";")
    queries = []
    
    for chunk in raw_chunks:
        trimmed = chunk.strip()
        if not trimmed:
            continue
            
        # Extract title from comments
        lines = trimmed.splitlines()
        comment_lines = []
        sql_lines = []
        
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("--"):
                clean_comment = stripped.lstrip("-").strip()
                if clean_comment and not set(clean_comment) <= {"="}:
                    comment_lines.append(clean_comment)
            else:
                sql_lines.append(line)
                
        sql_body = "\n".join(sql_lines).strip()
        if not sql_body or sql_body.upper().startswith("USE "):
            continue
            
        title = comment_lines[0] if comment_lines else "Analytical Query"
        queries.append((title, sql_body))
        
    return queries

def run_analytical_queries(spark):
    """
    Executes analytical queries from hive/queries/analytical_queries.sql
    and generates an execution report.
    """
    sql_file = PROJECT_ROOT / "hive" / "queries" / "analytical_queries.sql"
    results_dir = PROJECT_ROOT / "hive" / "query_results"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Loading Hive analytical SQL queries from: {sql_file}")
    queries = parse_sql_file(sql_file)
    logger.info(f"Parsed {len(queries)} analytical queries.")
    
    spark.sql(f"USE {HIVE_DATABASE_NAME}")
    
    summary_report = []
    summary_report.append(f"# HiveQL Analytical Query Execution Report\n")
    summary_report.append(f"**Database:** `{HIVE_DATABASE_NAME}`  \n")
    summary_report.append(f"**Total Queries Executed:** {len(queries)}  \n")
    summary_report.append(f"**Execution Timestamp:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    summary_report.append("---\n\n")
    
    for idx, (title, sql_query) in enumerate(queries, start=1):
        logger.info(f"\n==================================================")
        logger.info(f"Query {idx}: {title}")
        logger.info(f"==================================================")
        
        t0 = time.time()
        result_df = spark.sql(sql_query)
        record_count = result_df.count()
        exec_time = round(time.time() - t0, 3)
        
        logger.info(f"Completed in {exec_time}s | Returned: {record_count} rows")
        result_df.show(20, truncate=False)
        
        # Append to report
        summary_report.append(f"## Query {idx}: {title}\n\n")
        summary_report.append(f"- **Execution Time:** {exec_time} seconds\n")
        summary_report.append(f"- **Record Count:** {record_count}\n\n")
        summary_report.append("```sql\n" + sql_query + "\n```\n\n")
        summary_report.append("**Query Output:**\n\n")
        
        pdf = result_df.limit(20).toPandas()
        summary_report.append("```\n" + pdf.to_string(index=False) + "\n```\n\n")
        summary_report.append("---\n\n")
        
    report_path = results_dir / "hive_query_execution_report.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.writelines(summary_report)
        
    logger.info(f"All {len(queries)} queries executed! Full report saved to: {report_path}")

if __name__ == "__main__":
    spark = get_spark_session("Hive-Query-Runner", enable_hive=True)
    try:
        init_hive_schema(spark)
        run_analytical_queries(spark)
    finally:
        spark.stop()
