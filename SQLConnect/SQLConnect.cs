using System;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace SQLConnectLibrary
{
    /// <summary>
    /// Use this class in conjunction with the QueryHelper class to connect with databases.  It returns either a result set as a two dimensional array or as a serializable object that will be turned into JSON by remoteq.aspx.cs
    /// </summary>
    public class SQLConnect
    {
        private String serverName = "";
        private String databaseName = "";
        private String connectionString = "";
        public enum connectionType
        {
            /// <summary>
            /// OLE connection such as MSAccess
            /// </summary>
            OleConnection,
            /// <summary>
            /// ODBC Connection such as AS400
            /// </summary>
            OdbcConnection,
            /// <summary>
            /// Typical MSSQL connection
            /// </summary>
            SqlConnection,
            /// <summary>
            /// Stored procedure
            /// </summary>
            SqlStoredProcedureConnection
        }
        public enum outputType
        {
            /// <summary>
            /// No result set
            /// </summary>
            noOutput,
            /// <summary>
            /// for use with remoteQ.aspx only
            /// </summary>
            forRemoteQ,
            /// <summary>
            /// returns String[*,*]
            /// </summary>
            arrayOutput
        }
        /// <summary>
        /// The result set when output type is arrayOutput
        /// </summary>
        public String[,] results;
        /// <summary>
        /// Column Names when output type is arrayOutput
        /// </summary>
        public String[] columnNames;
        /// <summary>
        /// The result set when output type is forRemoteQ
        /// </summary>
        public DataOut DOresults;
        /// <summary>
        /// Any errors if SQL statement will not complete
        /// </summary>
        public String ErrorMessage = "";
        /// <summary>
        /// the default connection time out for connection types SqlConnection and SqlStoredProcedureConnection
        /// </summary>
        public Decimal connectionTimeOut = 300;
        /// <summary>
        /// Creates a new instance of SQLConnect
        /// </summary>
        /// <param name="_ServerName">Ignored if not querying MSSQL Server. The name of the MSSQL server</param>
        /// <param name="_databaseName">Ignored if not querying MSSQL Server. The database name</param>
        public SQLConnect(String _ServerName = "", String _databaseName = "")
        {
            serverName = _ServerName;
            databaseName = _databaseName;
        }
        /// <summary>
        /// Runs a stored procedure query on a SQL server
        /// </summary>
        /// <param name="StoredProcedureName">The name of the stored procedure, pr_ is automatically appended as the prefix if not already present</param>
        /// <param name="parameters">Stored Procedure parameters</param>
        /// <param name="_outputType">(Optional)Type of output</param>
        /// <returns>True if command is successful</returns>
        public Boolean runSPQuery(String StoredProcedureName, String[] parameters, outputType _outputType = outputType.noOutput)
        {
            String SQLStatement = getSQLStoreProcedure(StoredProcedureName, parameters);
            if (SQLStatement == "")
            {
                ErrorMessage = "Could not find stored procedure " + StoredProcedureName + " on Server " + serverName + " database " + databaseName;
                return false;
            }
            return runMSSQLQuery(SQLStatement, _outputType);
        }
        /// <summary>
        /// Execute a SQL command on MSSQL Server
        /// </summary>
        /// <param name="SQLStatement">The SQL Command</param>
        /// <param name="_outputType">(Optional)Type of output</param>
        /// <returns>True if command is successful</returns>
        public Boolean runMSSQLQuery(String SQLStatement, outputType _outputType = outputType.noOutput){
            getMSSQLConnectionString();
            return executeCommand(SQLStatement, connectionType.SqlConnection, _outputType);

        }
        /// <summary>
        /// Execute a SQL command on any type of connection
        /// </summary>
        /// <param name="ConnectionString">The Connection string</param>
        /// <param name="SQLStatement">The SQL command</param>
        /// <param name="_connectionType">The Connection Type</param>
        /// <param name="_outputType">The output type</param>
        /// <returns>True if command is successful</returns>
        public Boolean runQuery(String _ConnectionString, String SQLStatement, connectionType _connectionType, outputType _outputType = outputType.noOutput)
        {
            connectionString = _ConnectionString;
            return executeCommand(SQLStatement, _connectionType, _outputType);
        }
        /// <summary>
        /// Removes single quote from any provided parameters to prevent SQL injections
        /// </summary>
        /// <param name="optvals">By ref, The String array containing the parameters</param>
        public static void killSQLInjections(ref String[] optvals)
        {
            if (optvals != null && optvals.GetUpperBound(0) >= 0)
            {
                for (int i = 0; i <= optvals.GetUpperBound(0); i++)
                {
                    if (optvals[i] != null) { optvals[i] = optvals[i].Replace("'", ""); }
                }
            }
        }
        /// <summary>
        /// Creates a Dataout object from column names array and results array
        /// </summary>
        /// <param name="columnNames">Array of String with column names</param>
        /// <param name="results">the results set</param>
        /// <returns></returns>
        public static DataOut createDataOutObject(String[] columnNames, String[,] results)
        {
            var dOut = new DataOut();
            if (results != null)
            {
                dOut.columns = columnNames;
                for (int i = 0; i < results.GetLength(1); i++)
                {
                    var thisRow = new String[results.GetLength(0)];
                    for (int n = 0; n < results.GetLength(0); n++)
                    {
                        thisRow[n] = results[n, i];
                    }
                    dOut.rows.Add(thisRow);
                }
            }
            return dOut;
        }
        /// <summary>
        /// The class used by remoteq.aspx
        /// </summary>
        public class DataOut
        {
            /// <summary>
            /// object that will be converted to string (serialized) JSON object
            /// </summary>
            public List<String[]> rows = new List<string[]>();
            /// <summary>
            /// Column names
            /// </summary>
            public String[] columns = null;
        }
        //****************Private**************************************************
        private String getSQLStoreProcedure(String spName, String[] _parameters)
        {
            //kill SQL injections
            killSQLInjections(ref _parameters);
            if (spName.Length > 3 && spName.Substring(0, 3) != "pr_") { spName = "pr_" + spName; }


            if (!runMSSQLQuery("SELECT CASE  WHEN t.name IN ('tinyint', 'smallint', 'real', 'money', 'float', 'bit', 'decimal', 'numeric', 'smallmoney', 'bigint', 'int') THEN 0 ELSE 1 END [needsQuote]	FROM " + databaseName + ".sys.objects o INNER JOIN " + databaseName + ".sys.parameters p ON p.object_id = o.object_id INNER JOIN " + databaseName + ".sys.types t ON t.user_type_id = p.user_type_id WHERE o.name = '" + spName + "'", outputType.arrayOutput))
            {
                return "";
            }
            if (results != null && _parameters != null && results.GetLength(1) > _parameters.GetLength(0)) { return ""; }
            String parameters = "";
            if (results != null)
            {
                for (int i = 0; i < results.GetLength(1); i++)
                {
                    parameters += (parameters == "" ? "" : ", ");
                    if (results[0, i] == "1" && _parameters[i] != "NULL")
                    {
                        parameters += "'" + _parameters[i] + "'";
                    }
                    else
                    {
                        parameters += _parameters[i];
                    }
                }
            }
            return "EXEC " + databaseName + ".dbo." + spName + " " + parameters;
        }
        private void getMSSQLConnectionString()
        {
            connectionString = "workstation id=fasintra1;packet size=4096;user id=webapps;data source=" + serverName + ";persist security info=True;initial catalog=" + databaseName + ";password=W3b4pp5;Connect Timeout=" + connectionTimeOut;
        }
        private void setColumnNames(DbDataReader CommonReader, outputType _outputType)
        {
            var CN = new List<String>();
            for (int i = 0; i < CommonReader.FieldCount; i++)
            {
                CN.Add(CommonReader.GetName(i));
            }
            switch (_outputType)
            {
                case outputType.arrayOutput:
                    columnNames = CN.ToArray();
                    break;
                case outputType.forRemoteQ:
                    DOresults.columns = CN.ToArray();
                    break;
            }
        }
        private Boolean executeCommand(String SQLStatement, connectionType _connectionType, outputType _outputType)
        {
            ErrorMessage = "";
            columnNames = null;
            DOresults = new DataOut();
            results = null;

            DbDataReader commonReader = null;
            DbConnection commonConnection = null;

            Boolean noResults = (_outputType == outputType.noOutput);
            try
            {
                switch (_connectionType)
                {
                    case connectionType.OleConnection:
                        var connection1 = new OleDbConnection(connectionString);
                        var command1 = new OleDbCommand(SQLStatement, connection1);
                        connection1.Open();
                        if (noResults)
                        {
                            command1.ExecuteNonQuery();
                        }
                        else
                        {
                            commonReader = (DbDataReader)command1.ExecuteReader(CommandBehavior.CloseConnection);
                        }
                        commonConnection = (DbConnection)connection1;
                        break;
                    case connectionType.OdbcConnection:
                        var connection2 = new OdbcConnection(connectionString);
                        var command2 = new OdbcCommand(SQLStatement, connection2);
                        connection2.Open();
                        if (noResults)
                        {
                            command2.ExecuteNonQuery();
                        }
                        else
                        {
                            commonReader = (DbDataReader)command2.ExecuteReader(CommandBehavior.CloseConnection);
                        }
                        commonConnection = (DbConnection)connection2;
                        break;
                    case connectionType.SqlConnection:
                    case connectionType.SqlStoredProcedureConnection:
                        var connection3 = new SqlConnection(connectionString);
                        var command3 = new SqlCommand(SQLStatement, connection3);
                        connection3.Open();
                        if (noResults)
                        {
                            command3.ExecuteNonQuery();
                        }
                        else
                        {
                            commonReader = (DbDataReader)command3.ExecuteReader(CommandBehavior.CloseConnection);
                        }
                        commonConnection = (DbConnection)connection3;
                        break;
                }
                if (noResults) { return true; }
                if (commonReader == null)
                {
                    ErrorMessage = "Lost Reader";
                    return false;
                }
                setColumnNames(commonReader, _outputType);
                var resultsOut = new List<String[]>();
                while (commonReader.Read())
                {
                    var thisRow = new String[commonReader.FieldCount];
                    for (int i = 0; i < thisRow.GetLength(0); i++)
                    {
                        if (!commonReader.IsDBNull(i))
                        {
                            var thisValue = commonReader.GetValue(i);
                            if (thisValue.GetType() == typeof(Byte[]))
                            {
                                thisValue = byteToString((Byte[])thisValue);
                            }
                            thisRow[i] = thisValue.ToString().Replace("&#39;", "'");
                        }
                    }
                    resultsOut.Add(thisRow);
                }
                commonReader.Close();
                if (commonConnection.State == ConnectionState.Open)
                {
                    commonConnection.Close();
                }
                if (resultsOut.Count > 0)
                {
                    switch (_outputType)
                    {
                        case outputType.arrayOutput:
                            setResults(resultsOut);
                            break;
                        case outputType.forRemoteQ:
                            DOresults.rows = resultsOut;
                            break;
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                ErrorMessage = SQLStatement + ": " + ex.ToString();
                return false;
            }
        }
        private void setResults(List<String[]> lResults)
        {
            for (int i = 0; i < lResults.Count; i++)
            {
                var thisRow = lResults[i];
                if (i == 0)
                {
                    results = new String[thisRow.Length, lResults.Count];             
                }
                for (int n = 0; n < thisRow.Length; n++)
                {
                    results[n, i] = thisRow[n];
                }
            }
        }
        private static String byteToString(Byte[] byteIn)
        {
            var strOut = new StringBuilder();
            if (byteIn != null && byteIn.Length > 0)
            {
                for (int i = 0; i < byteIn.Length; i++)
                {
                    strOut.Append(byteIn[i].ToString("x").PadLeft(2, '0'));
                }
            }
            return strOut.ToString();
        }
    }
}
