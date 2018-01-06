import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.BatchUpdateException

fun main(args: Array<String>) {


//        Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe", "rpe", "rpe")
        val connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/XE", "system", "oracle")
        connection.setAutoCommit(false)

        dropTableIfExists(connection)

        createTable(connection)
        connection.commit()

        insertRow(connection, 5) // insert with row 5
        insertRow(connection, 8) // insert with row 8
        connection.commit()

        dumpTableContents(connection)

        val numbers = listOf(0,1,2,3,4,5,6,7,8,9)

        insertBatchIgnoringDuplicateKeysAndParentKeyNotFoundErrors(connection, numbers)

        dumpTableContents(connection)

        dropTableIfExists(connection)
        connection.commit()

    }

fun createTable(connection: Connection)  {
    val statement = connection.createStatement()
    val cresql = "create table kje_test(id number(3) primary key, field varchar2(30))"
    statement.execute(cresql)
    statement.close()
}

fun dumpTableContents(connection: Connection)  {
    val queryStatement = connection.createStatement()
    val rs = queryStatement.executeQuery("select id, field from kje_test")

    while (rs.next()) {
        System.out.println("Rij " + rs.getInt(1) + " + " + rs.getString(2))
    }
    rs.close()
}

fun insertRow(connection: Connection, id: Int) {
    val insertStatement = connection.prepareStatement("insert into kje_test values(?, ?)")

    insertStatement.setInt(1, id)
    insertStatement.setString(2, "AUTO_GEN")
    insertStatement.execute()
}

fun dropTableIfExists(connection: Connection)  {
    try {
        val dropStatement = connection.createStatement()
        dropStatement.execute("drop table kje_test")
    } catch (sqle: SQLException) {
        System.out.println("Table cannot be dropped.")
    }

}

fun insertBatchIgnoringDuplicateKeysAndParentKeyNotFoundErrors(connection: Connection, batch: List<Int>) {
    val insertStatement = connection.prepareStatement("insert into kje_test values(?, ?)")
    var rowsProcessed = 0

    while (rowsProcessed < batch.size){
        try {
            val subbatch = batch.subList(rowsProcessed, batch.size)
            for (b in subbatch) {
                insertStatement.setInt(1, b)
                insertStatement.setString(2, "Hithere")
                insertStatement.addBatch()
            }

            insertStatement.executeBatch()
            rowsProcessed += insertStatement.getUpdateCount()
            insertStatement.close()

        } catch (bue: BatchUpdateException) {
            val updateCount = insertStatement.getUpdateCount()
            rowsProcessed += updateCount
            System.out.println("insertStatement.getUpdateCounts() returns " + updateCount)
            val excmsg = bue.message.orEmpty()
            if (excmsg.startsWith("ORA-00001:")) {
                System.out.println("ORA-00001 (duplicate key) error occured at index=" + rowsProcessed + " id=" + batch[rowsProcessed])
            } else if (excmsg.startsWith("ORA-02291")) {
                System.out.println("ORA-02291 (parent key not found) error occured at index=" + rowsProcessed + " id=" + batch[rowsProcessed])
            } else {
                System.out.println("Unexpected error " + excmsg + " occured at index=" + rowsProcessed + " id=" + batch[rowsProcessed])
            }
            System.out.println("Ignoring index " + rowsProcessed)
            rowsProcessed++
        }
    }
    connection.commit()
}

