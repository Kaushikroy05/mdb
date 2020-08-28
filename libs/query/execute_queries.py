import logging
import math
import multiprocessing
from .session import DbSession

log = logging.getLogger(__name__)

NUM_PARALLEL_PROCS = 16


class ExecuteQueries:
    def __init__(self, conn_params, procs=NUM_PARALLEL_PROCS):
        self.conn_params = conn_params
        self.db_session = None
        self.procs = procs

    def _get_session(self):
        return DbSession(
            self.conn_params,
            isolate_db=False
        )

    def execute_queries(self, queries, fetch_result=False):
        db = self._get_session()
        cursor = db.cursor()

        for query in queries:
            cursor.execute(query)

        if fetch_result:
            out = cursor.fetchall()
            log.info("Query ({}) output \n {}".format(query, out))
            return out
        else:
            log.info("Query ({})".format(query))


    def execute_parallel_queries(self, list_queries, timeout=7200):

        queries_per_job = int(math.ceil(
            len(list_queries) / float(self.procs)
        ))

        job_query_list = [
            list_queries[i * queries_per_job: (i + 1) * queries_per_job]
            for i in range(
                (len(list_queries) + queries_per_job - 1) // queries_per_job
            )
        ]

        jobs = list()

        for x in range(0, len(job_query_list)):
            proc = multiprocessing.Process(
                target=self.execute_queries,
                name="Run-Query-{}".format(x),
                args=(
                    [ job_query_list[x] ]
                )
            )
            jobs.append(proc)

        for job in jobs:
            job.start()
        for job in jobs:
            job.join()