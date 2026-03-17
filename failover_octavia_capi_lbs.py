"""
This script checks for loadbalancers created by cluster-api
and if any loadbalancer is in ERROR state; automatically
failover that loadbalancer
"""

import argparse
import re
import sys
import time
import logging
import os
from typing import List, Tuple, Optional
from logging.handlers import RotatingFileHandler

try:
  from openstack import connection
except ImportError:
    print("ERROR: openstacksdk not found. Install with: pip install openstacksdk", file=sys.stderr)
    sys.exit(1)


# ----------------------------
# Logging
# ----------------------------
def get_file_logger(level: int = logging.INFO) -> logging.Logger:
    """
    Create a logger that writes to a file (with fallback)
    """
    logger = logging.getLogger("capi_lb_failover")
    logger.setLevel(level)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    # Preferred log file
    orig_log_file = "/var/log/capi_lb_failover.log"
    log_file = orig_log_file

    # Fallback to user's home directory if required
    try:
        with open(orig_log_file, "a"):
            pass
    except Exception:
        log_file = os.path.expanduser("~/.capi_lb_failover.log")


    # set the parameters for the file handler
    fh = RotatingFileHandler(log_file, maxBytes=100 * 1024 * 1024, backupCount=3)
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
    logger.addHandler(fh)

    return logger


# ----------------------------
# OpenStack connection
# ----------------------------
def get_connection(cloud: Optional[str] = None):
    """
    Build a conection from clouds.yaml (if --cloud is provided),
    otherwise from ENV vars
    """
    if cloud:
        return connection.from_config(cloud=cloud)
    # Fall back to env vars if --cloud arg is not provided
    return connection.from_config() # from_config also respects env when cloud is None



# ----------------------------
# Discovery & actions
# ----------------------------
def list_capi_error_lbs(conn, all_projects: bool, name_regex: re.Pattern):
    """
    Returns the list of lb(s) from all the projects which are in
    provisioning state ERROR and matching the name_regex
    """
    logger = get_file_logger()
    query = {"provisioning_status": "ERROR"}
    logger.info("Listing load balancers with query=%s all_projects=%s", query, all_projects)

    lbs = []

    try:
        lbs_iter = conn.load_balancer.load_balancers(all_projects=all_projects, **query)
        lbs = list(lbs_iter)
    except Exception as e:
        logger.exception("Failed to list loadbalancers: %s", e)
        sys.exit(1)
    
    # Filter the lb(s) based on the name_regex
    logger.info("Filtering load balancers in ERROR state with match the regex pattern %s", name_regex.pattern)
    filtered_lbs = []
    for lb in lbs:
        name = getattr(lb, "name", "") or ""
        if name_regex.match(name):
            filtered_lbs.append(lb)

    return filtered_lbs


def failover_capi_error_lb(conn, lb_id: str):
    """
    Attempts to trigger a failover on the given loadbalancer ID
    Returns (success, message).
    """
    logger = get_file_logger()
    logger.info("Initiating failover for load balancer %s", lb_id)
    
    try:
        conn.load_balancer.failover_load_balancer(lb_id)
        logger.info("Failover initiated successfully for load balancer %s", lb_id)
        return True, "failover initiated"
    except Exception as e:
        logger.exception("Failover failed for load balancer id=%s with exception: %s", lb_id, e)
        return False, f"failover failed: {e}"

def refresh_capi_error_lb_state(conn, lb_id: str):
    """
    Attempts to obtain the new state of the lb 
    post successful failover attempt
    """

    logger = get_file_logger()
    logger.info("Attempting to obtain the new state of load balancer %s post failover", lb_id)

    try:
        return conn.load_balancer.get_load_balancer(lb_id)
    except Exception as e:
        logger.exception("Failed to obtain the state of loadbalancer %s with exception: %s", lb_id, e)
        return None


# ----------------------------
# CLI & main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Failover Octavia load balancers stuck in provisioning ERROR.")
    parser.add_argument("--cloud", help="Cloud name from clouds.yaml (optional).")
    parser.add_argument("--limit", type=int, default=5,
                        help="Max number of load balancers to failover in this run. Default: 5")
    parser.add_argument("--sleep-between", type=int, default=2,
                        help="Seconds to sleep between successive failover calls. Default: 2")
    parser.add_argument("--post-wait", type=int, default=60,
                        help="Seconds to wait after failover before checking state. Default: 60 (1 minute)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't actually failover; just print which LBs would be affected.")

    args = parser.parse_args()
    logger = get_file_logger()

    if args.limit > 10:
        logger.error("LB failover limit can not be higher than 10")
        sys.exit(1)
    
    try:
        conn = get_connection(args.cloud)
    except Exception as e:
        logger.exception("Failed to build OpenStack connection %s", e)
        sys.exit(1)

    logger.info("Connected to Octavia API")
     
    capi_lbs = list_capi_error_lbs(conn, True, re.compile(r'^k8s-clusterapi'))

    if not capi_lbs:
        logger.info("No capi loadbalancers found in provisioning state ERROR")
        return

    logger.info("List of capi lb(s) in ERROR state")

    # sort the lb(s)
    try:
        capi_lbs.sort(key=lambda lb: getattr(lb, "updated_at", "") or getattr(lb, "created_at", "") or "")
    except Exception:
        pass

    logger.info("LB names: %s", [getattr(capi_lb, "name", "") for capi_lb in capi_lbs])

    # initialize the counter
    counter = 0
    for capi_lb in capi_lbs:
        if counter >= args.limit:
            break
        
        # obtain the lb id
        lb_id = getattr(capi_lb, "id")
        lb_name = getattr(capi_lb, "name", "") or ""

        if not lb_id:
            logger.warning("Skipping LB with missing id (name=%s)", lb_name)
            counter += 1 # count increment to prevent endless looping
            continue # skip the rest of the loop steps; there is no lb id

        prov_state = getattr(capi_lb, "provisioning_status", "")
        logger.info("Target LB id=%s name='%s' provisioning_status=%s", lb_id, lb_name, prov_state)
        
        if args.dry_run:
            logger.info("DRY RUN: no failover will be triggered for lb %s", lb_id)
            counter +=1 # count increment to prevent endless looping
            time.sleep(max(0, args.sleep_between)) 
            continue # skip the rest of the loop as it's only dry run
        
        # trigger the failover for the lb
        ok, msg = failover_capi_error_lb(conn, lb_id)
        if not ok:
            counter +=1 # increment the counter even on failure to prevent endless looping
            time.sleep(max(0, args.sleep_between)) # prevent hammering the api
            # failover failed skip the rest of the loop and try the other lb(s)
            continue 

        # increment the counter; failover request was accepted by the api
        counter +=1
        # prevent hammering the api
        time.sleep(max(0, args.sleep_between))

    logger.info("All required lb(s) failover sucessful")

if __name__ == "__main__":
    main()
