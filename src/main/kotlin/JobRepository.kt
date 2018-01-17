package commons

import java.sql.*
import kotlin.coroutines.experimental.buildSequence

import kotlin.test.assertNotNull

val SelectAndLockJob =
"""declare
 dummy number;
 jobfound boolean := false;
 cursor c_check(p_jobid number) is
   select 1 from pas_jobs where id = p_jobid and ind_verwerken = 'J' for update nowait;
 job_is_exclusive_locked exception;
 pragma exception_init(job_is_exclusive_locked, -54);
 cursor c_main is
   select *
   from (select jobs.id, batches.id as bdeId, batches.prioriteit, batches.job_grootte
         from pas_jobs jobs
         inner join pas_batch_definities batches
         on batches.id = jobs.bde_id
         where ind_verwerken = 'J'
         and (aantal_keer_geprobeerd < :max_retries1 or 0 = :max_retries2)
         order by batches.prioriteit asc, batches.id asc
        )
   where rownum <= 100;
begin
  for r in c_main
  loop
    begin
      open c_check(r.id);
      fetch c_check into dummy;
      jobfound := c_check%found;
      close c_check;
      if jobfound then
        :jobId := r.id;
        :bdeId := r.bdeId;
        :prioriteit := r.prioriteit;
        :job_grootte := r.job_grootte;
        :result_code := 1;
        return;
      end if;
    exception
      when job_is_exclusive_locked then
        null;
    end;
  end loop;
end;
"""

val updateJobToProcessing = """
update pas_jobs
set ind_verwerken = 'P'
,   startdatum = ?
,   aantal_keer_geprobeerd = nvl(aantal_keer_geprobeerd, 0) + 1
where id = ?
"""

val updateJobToFinished = """
update pas_jobs
set ind_verwerken = 'N'
,   einddatum = ?
where id = ?
"""

val updateJobToNogVerwerken = """
update pas_jobs
set ind_verwerken = 'J'
where id = ?
"""

val selectJobOnId = """
select ind_verwerken
from   pas_jobs
where  id = ?
for update nowait
"""

val selectJobsInProgress = """
select id, startdatum
from   pas_jobs
where  ind_verwerken = 'P'
"""

val selectJobOnIdAllReady = """
select 1
from   pas_jobs j
where  j.id = ?
and    not exists
       ( select 1
         from   pas_formulariumvragen v
         where  v.job_id = j.id
         and    not exists ( select 1
                             from   pas_fmg_fouten f
                             where  f.fmg_id = v.id
                           )
         and    not exists ( select 1
                             from   pas_resultaten r
                             where  r.rst_id = v.id
                           )
       )
"""

val selectVragen =
        """select fmg.id,
fmg.job_id,
fmg.opgenomen_productpakket_id,
fmg.dienstverband_id,
fmg.associatie_id,
fmg.berekenen_datum,
fmg.formularium_peildatum,
fmg.startdatum,
fmg.registratie_datumtijd,
fmg.flexrekendatum,
fmg.antwoorddetail,
fmg.vraagsoort,
fmg.bde_id
from pas_formulariumvragen fmg
where job_id = ?
and    not exists ( select 1
                    from   pas_fmg_fouten f
                    where  f.fmg_id = fmg.id
                   )
and    not exists ( select 1
                    from   pas_resultaten r
                    where  r.rst_id = fmg.id
                  )
"""

private val insertResultaat =
"""insert into pas_resultaten
( RST_ID
, DVD_ID
, BEREKENDATUM
, ASE_ID
, NANTWOORD
, PROD_RST
, AANTAL_NRST
, STATUS
, OPT_ID
, VRAAGSOORT
, BDE_ID
, STARTDATUM
, FLEXREKENDATUM
, JOB_ID
) values
( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

private val insertFout =
"""insert into pas_fmg_fouten
( FOUTMELDING
, FMG_ID
, BDE_ID
, JOB_ID
) values
( ?, ?, ?, ?)
"""

private val insertTussenresultaat =
"""insert into pas_tussenresultaten
( RST_ID
, TR_NAAM
, TRRESULTAAT
, BDE_ID
, JOB_ID
) values
( ?, ?, ?, ?, ?)
"""


data class VraagData(val id: Long, val job_id: Long, val opt_id: Long, val dvd_id: Long, val ase_id: Long?
                     , val berekenen_datum: Date, val formularium_peildatum: Date?, val startdatum: Date?
                     , val registratie_datumtijd: Timestamp?, val flexrekendatum: String?
                     , val antwoorddetail: Int, val vraagsoort: String, val bde_id: Long)


data class Job(val batchDefinitieId: Long, val jobId: Long, val priority: Int, val jobGrootte: Int)

data class JobRestart(val jobId: Long, val starttime: Timestamp)

class JobVragenConnection(address: String, user: String, pass: String) {
    private val connection = DriverManager.getConnection(address, user, pass)
    private val getJobStmt = connection.prepareCall(SelectAndLockJob)
    private val updateJobStmt = connection.prepareStatement(updateJobToProcessing)
    private val queryVragenStmt = connection.prepareStatement(selectVragen)
    private val selectJobStmt = connection.prepareStatement(selectJobOnId)
    private val selectJobReadyStmt = connection.prepareStatement(selectJobOnIdAllReady)
    private val updateJobFinishedStmt = connection.prepareStatement(updateJobToFinished)
    private val updateJobToVerwerkenStmt = connection.prepareStatement(updateJobToNogVerwerken)
    private val selectJobsInProgressStmt = connection.prepareStatement(selectJobsInProgress)


    init {
        connection.setAutoCommit(false)
    }

    fun getAndLockJob(maxRetries: Int): Job? {
        getJobStmt.setInt("max_retries1", maxRetries)
        getJobStmt.setInt("max_retries2", maxRetries)
        getJobStmt.registerOutParameter("jobId", Types.NUMERIC)
        getJobStmt.registerOutParameter("bdeId", Types.NUMERIC)
        getJobStmt.registerOutParameter("prioriteit", Types.NUMERIC)
        getJobStmt.registerOutParameter("job_grootte", Types.NUMERIC)
        getJobStmt.registerOutParameter("result_code", Types.NUMERIC)

        getJobStmt.execute()

        val jobId = getJobStmt.getLong("jobId")
        val bdeId = getJobStmt.getLong("bdeId")
        val prioriteit = getJobStmt.getInt("prioriteit")
        val jobGrootte = getJobStmt.getInt("job_grootte")
        val resutltCode = getJobStmt.getInt("result_code")

        if (!resutltCode.equals(1)) return null
        return Job(bdeId, jobId, prioriteit, jobGrootte)
    }


    fun updateJobToPosted(jobId: Long) {

        updateJobStmt.setTimestamp(1, Timestamp(System.currentTimeMillis()))
        updateJobStmt.setLong(2, jobId)
        updateJobStmt.execute()
        if (!updateJobStmt.updateCount.equals(1)) {
            println("Warning when updating job ${jobId} to posted, rows updated=${updateJobStmt.updateCount}. Should've been 1.")
        }
        connection.commit()
    }

    fun rollback() = connection.rollback()
    fun commit() = connection.commit()

    fun selectJobVragen(jobId: Long?) = buildSequence {

        if (jobId != null) {
            queryVragenStmt.setLong(1, jobId)
            val rs = queryVragenStmt.executeQuery()

            while (rs.next()) {
                yield(VraagData(rs.getLong(1),
                        rs.getLong(2),
                        rs.getLong(3),
                        rs.getLong(4),
                        rs.getLong(5),
                        rs.getDate(6),
                        rs.getDate(7),
                        rs.getDate(8),
                        rs.getTimestamp(9),
                        rs.getString(10),
                        rs.getInt(11),
                        rs.getString(12),
                        rs.getLong(13)))
            }
            rs.close()
        }
    }.toList()

    fun selectJobVsInProgress() = buildSequence {
        val rs = selectJobsInProgressStmt.executeQuery()

        while (rs.next()) {
            yield(JobRestart(rs.getLong(1), rs.getTimestamp(2)))
        }
        rs.close()
    }.toList()

    fun pleasePostJobCheckReadyAgain(jobId: Long): Boolean {
        selectJobStmt.setLong(1, jobId)
        var ind_verwerken = ""
        try {
            val rs = selectJobStmt.executeQuery()
            if (!rs.next()) {
                println("Job with id ${jobId} does not exists. Strange, but no action needed")
                rs.close()
                return false
            }
            ind_verwerken = rs.getString(1)
            rs.close()
        } catch (e: SQLException) {
            if (e.errorCode == 54) {
                println("Cannot lock record of job with id ${jobId}. ORA-00054. Retrying later")
            } else {
                println("Unexpected error when trying to lock job with id ${jobId}:\n${e}")
            }
            return true
        }
        if (!ind_verwerken.equals("P")) {
            println("Job with id ${jobId} has ind_verwerken=${ind_verwerken}. No action needed")
            connection.commit()
            return false
        }
        selectJobReadyStmt.setLong(1, jobId)
        val rs = selectJobReadyStmt.executeQuery()
        if (!rs.next()) {
            println("Job with id ${jobId} is not ready yet!")
            rs.close()
            connection.commit()
            return true
        }
        rs.close()
        println("Job with id ${jobId} is ready (means no vragen unanswered)")

        updateJobFinishedStmt.setTimestamp(1, Timestamp(System.currentTimeMillis()))
        updateJobFinishedStmt.setLong(2, jobId)
        updateJobFinishedStmt.execute()
        if (!updateJobFinishedStmt.updateCount.equals(1)) {
            println("Warning when updating job ${jobId} to finished, rows updated=${updateJobFinishedStmt.updateCount}. Should've been 1.")
        }
        connection.commit()

        return false
    }


    fun postJobToVerwerken(jobId: Long) {
        updateJobToVerwerkenStmt.setLong(1, jobId)
        updateJobToVerwerkenStmt.execute()
        if (!updateJobToVerwerkenStmt.updateCount.equals(1)) {
            println("Warning when updating job ${jobId} to verwerken, rows updated=${updateJobToVerwerkenStmt.updateCount}. Should've been 1.")
        }
        connection.commit()
    }
}


class ResultsConnection(address: String, user: String, pass: String) {
    private val connection = DriverManager.getConnection(address, user, pass)
    private val insertFoutStmt = connection.prepareStatement(insertFout)
    private val insertResultaatStmt = connection.prepareStatement(insertResultaat)
    private val insertTussenresultaatStmt = connection.prepareStatement(insertTussenresultaat)

    init {
        connection.setAutoCommit(false)
    }

    fun insertIpdeResponses(results:  List<Results>) {
        results.forEach {
            insertFoutStmt.setLong(1, it.iets)
            insertFoutStmt.setLong(2, it.iets)
            insertFoutStmt.setLong(3, it.iets)
            insertFoutStmt.setLong(4, it.iets)
            insertFoutStmt.addBatch()
        }
        insertFoutStmt.execute()
        connection.commit()
    }



}