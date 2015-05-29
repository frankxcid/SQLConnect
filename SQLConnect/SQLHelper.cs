using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace SQLConnectLibrary
{
    public class SQLHelper
    {
        #region Public Properties
        /// <summary>
        /// Contains error messages if any
        /// </summary>
        public static String errMess = "";

        /// <summary>
        /// Contains the name of the column names
        /// </summary>
        public static String[] ColumnNames = null;

        /// <summary>
        /// Contains a 2 dimensional array that has all the results of the query in the form Array[columns, rows]
        /// </summary>
        public static String[,] Results = null;

        /// <summary>
        /// Contains the JSON object with the results of the query in the form of {"columns":[column_name0, column_name1, ...], "rows" : [[row0_col0_val, row0_col1_val, ...],[row1_col0_val, row1_col1_val, ...], ...]}
        /// </summary>
        public static String JSONResults = null;    

        /// <summary>
        /// Contains the name of the primary server to use when calling queries, set before calling queries
        /// </summary>
        public static String SQLServerName = "";

        /// <summary>
        /// Contains the name of the database to use when calling queries, set before calling queries
        /// </summary>
        public static String SQLServerDatabaseName = "";

        /// <summary>
        /// AS400 UserId, set before calling queries
        /// </summary>
        public static String AS400UID = "";

        /// <summary>
        /// AS400 Password, set before calling queries
        /// </summary>
        public static String AS400PWD = "";

        /// <summary>
        /// The types of queries. Used to select connection string
        /// </summary>
        public enum dbTypes
        {
            Access,
            BPCS,
            BPCSF,
            BPCS_PO,
            SQLServerStatement,
            SQLServerStoredProcedure
        }
        #endregion

        #region Public Methods
        /// <summary>
        /// Initializes Required values, all values are not required except when calling queries for SQL or AS400
        /// </summary>
        /// <param name="_SQLServerName">The name of the primary server to use when calling queries</param>
        /// <param name="_SQLServerDatabaseName">The name of the database to use when calling queries</param>
        /// <param name="_AS400UID">AS400 UserId</param>
        /// <param name="_AS400PWD">AS400 Password</param>
        public static void initialize(String _SQLServerName = "", String _SQLServerDatabaseName = "", String _AS400UID = "", String _AS400PWD = "")
        {
            SQLServerName = _SQLServerName;
            SQLServerDatabaseName = _SQLServerDatabaseName;
            AS400UID = _AS400UID;
            AS400PWD = _AS400PWD;
        }

        /// <summary>
        /// Adds SQLStatements that will be used with variables
        /// </summary>
        /// <param name="StatementName">The name used to call the statement. Name should be a single word</param>
        /// <param name="SQLStatement">The SQL Statement. Use substitute symbols that will be replaced in the SQL Statement: $s - Server Name, $d - Database Name, $p - Parameters (will be replaced in order)</param>
        /// <param name="databaseType">Defaults to Stored Procedure. The database type used to access connection string</param>
        /// <param name="adHocConnectionString">Optional connection string.  If present will override the default connection string</param>
        public static void addSQLStatement(String StatementName, String SQLStatement, dbTypes databaseType = dbTypes.SQLServerStoredProcedure, String adHocConnectionString = "")
        {
            var SQLObj = new SQLStatementObj();
            SQLObj.SQLStatement = SQLStatement;
            SQLObj.dbType = databaseType;
            SQLObj.ConnectionString = adHocConnectionString;
            SQLStatements.Add(StatementName, SQLObj);
        }

        /// <summary>
        /// Primarily used internally in SQLHelper but can be used to show how the SQLStatement will be constructed
        /// </summary>
        /// <param name="StatementName">The Name of the SQL Statement when added using SQLHelper.addSQLStatement() method</param>
        /// <param name="Parameters">String Array containing the variable parameters. </param>
        /// <returns>The completed constructed SQL Statement</returns>
        public static String getSQLStatement(String StatementName, String[] Parameters)
        {
            String strOut = "";
            SQLStatementObj SQLObj = null;
            if (Parameters != null) { SQLConnect.killSQLInjections(ref Parameters); }
            try
            {
                 SQLObj = SQLStatements[StatementName];
                 strOut = SQLObj.SQLStatement;
            }
            catch (KeyNotFoundException)
            {
                errMess = "SQL statement named " + StatementName + " does not exist. Add this statement using the SQLHelper.addSqlStatement() method";
                return "";
            }
            strOut = strOut.Replace("$s", SQLServerName);
            strOut = strOut.Replace("$d", SQLServerDatabaseName);
            String[] delim = { "$p" };
            var SQLParts = strOut.Split(delim, StringSplitOptions.None);
            if (SQLParts.Length == 1) { return strOut; }
            if (Parameters.Length < (SQLParts.Length - 1))
            {
                errMess = "Not enough parameters in SQL statement. Statement requires " + (SQLParts.Length - 1).ToString() + " parameters. Only " + Parameters.Length.ToString() + " parameter(s) provided: " + SQLObj.SQLStatement;
                return "";
            }
            strOut = "";
            for (var i = 0; i < SQLParts.Length; i++ )
            {
                strOut += SQLParts[i] + (i == SQLParts.GetUpperBound(0) ? "" : Parameters[i]);
            }
            return strOut;
        }

        /// <summary>
        /// Used to run queries added using SQLHelper.addSQLStatement() method
        /// </summary>
        /// <param name="SQLStatementName">The SQL Statement Name</param>
        /// <param name="Parameters">Parameters if any, otherwise null</param>
        /// <param name="_ouputType">Type of output</param>
        /// <returns></returns>
        public static Boolean doPreparedQuery(String SQLStatementName, String[] Parameters, SQLConnect.outputType _ouputType)
        {
            String SQLStatement = getSQLStatement(SQLStatementName, Parameters);
            SQLStatementObj SObj = SQLStatements[SQLStatementName];
            dbTypes databaseType = SObj.dbType;
            String ConnectionString = getConnectionString(databaseType);
            if (SObj.ConnectionString != null) { ConnectionString = SObj.ConnectionString; }
            return doQuery(SQLStatement, _ouputType, databaseType, ConnectionString);
        }

        /// <summary>
        /// Runs a query as sent to this method
        /// </summary>
        /// <param name="SQLStatement">The full SQL statement</param>
        /// <param name="_outputType">Type of output</param>
        /// <param name="_dbType">Specify the database type</param>
        /// <param name="ConnectionString">Connection String to use</param>
        /// <returns></returns>
        public static Boolean doQuery(String SQLStatement, SQLConnect.outputType _outputType, dbTypes _dbType, String ConnectionString)
        {
            return doWork(SQLStatement, _outputType, _dbType, ConnectionString);
        }

        /// <summary>
        /// Executes a stored procedure on Microsoft SQL server only.  
        /// </summary>
        /// <param name="storedProcedureName">The name of the stored procedure can be the whole name such as pr_DoWork, or the pr_ can be omitted such as DoWork</param>
        /// <param name="parameters">Stored procedure parameters in order</param>
        /// <param name="_outputType">type of output</param>
        /// <returns></returns>
        public static Boolean doSPQuery(String storedProcedureName, String[] parameters, SQLConnect.outputType _outputType)
        {
            optvals = parameters;
            return doWork(storedProcedureName, _outputType, dbTypes.SQLServerStoredProcedure);
        }

        /// <summary>
        /// Makes a copy of results so they are not referenced
        /// </summary>
        /// <returns>2-Dimensional Array</returns>
        public static String[,] cloneResults()
        {
            if (Results == null) { return null; }
            var strOut = new String[Results.GetLength(0), Results.GetLength(1)];
            for (int i = 0; i < Results.GetLength(1); i++)
            {
                for (int n = 0; n < Results.GetLength(0); n++)
                {
                    strOut[n, i] = Results[n, i];
                }
            }
            return strOut;
        }
        #endregion

        #region Private Global Variables
        private static String[] optvals = null;
        private static Dictionary<String, SQLStatementObj> SQLStatements = new Dictionary<string, SQLStatementObj>();
        #endregion

        #region Private Methods
        private class SQLStatementObj
        {
            public String SQLStatement = "";
            public dbTypes dbType = dbTypes.SQLServerStoredProcedure;
            public String ConnectionString = "";
        }


        private static String getConnectionString(dbTypes _dbType)
        {
            switch (_dbType)
            {
                case dbTypes.BPCS:
                    //return "DRIVER=iSeries Access ODBC Driver;COMPRESSION=0;LAZYCLOSE=1;PKG=BPCS405CDF/DEFAULT(IBM),2,0,1,0,512;LANGUAGEID=ENU;DFTPKGLIB=BPCS405CDF;DBQ=BPCS405CDF bpcscdusrt bpcscdusr bpcscdptf1 bpcs405cdf bpcs405cdo sequel sqlpgm qgpl qtemp;SYSTEM=NSW400;UID=" + AS400UID + ";PWD=" + AS400PWD;
                    return "DRIVER=iSeries Access ODBC Driver;COMPRESSION=0;LAZYCLOSE=1;PKG=TLXF/DEFAULT(IBM),2,0,1,0,512;LANGUAGEID=ENU;DFTPKGLIB=TLXF;DBQ=TLXF LXUSRF LXUSRO LXPTF LXO sequel sqlpgm qgpl qtemp;SYSTEM=NSW400;UID=" + AS400UID + ";PWD=" + AS400PWD;
                case dbTypes.BPCS_PO:
                    //return "DRIVER=iSeries Access ODBC Driver;COMPRESSION=0;LAZYCLOSE=1;PKG=QGPL/DEFAULT(IBM),2,0,1,0,512;LANGUAGEID=ENU;DFTPKGLIB=QGPL;DBQ=BPCSCDUSRF bpcscdusr bpcscdptf1 bpcs405cdf bpcs405cdo sequel sqlpgm qgpl qtemp;SYSTEM=NSW400;UID=" + AS400UID + ";PWD=" + AS400PWD;
                    return "DRIVER=iSeries Access ODBC Driver;COMPRESSION=0;LAZYCLOSE=1;PKG=QGPL/DEFAULT(IBM),2,0,1,0,512;LANGUAGEID=ENU;DFTPKGLIB=QGPL;DBQ=LXUSRF LXUSRO LXPTF TLXF LXO sequel sqlpgm qgpl qtemp;SYSTEM=NSW400;UID=" + AS400UID + ";PWD=" + AS400PWD;
                case dbTypes.BPCSF:
                    //return "DRIVER=iSeries Access ODBC Driver;COMPRESSION=0;LAZYCLOSE=1;PKG=BPCSCDUSRF/DEFAULT(IBM),2,0,1,0,512;LANGUAGEID=ENU;DFTPKGLIB=BPCSCDUSRF;DBQ=BPCSCDUSRF;SYSTEM=NSW400;UID=" + AS400UID + ";PWD=" + AS400PWD;
                    return "DRIVER=iSeries Access ODBC Driver;COMPRESSION=0;LAZYCLOSE=1;PKG=LXUSRF/DEFAULT(IBM),2,0,1,0,512;LANGUAGEID=ENU;DFTPKGLIB=LXUSRF;DBQ=LXUSRF;SYSTEM=NSW400;UID=" + AS400UID + ";PWD=" + AS400PWD;

            }
            return "";
        }
        private static SQLConnect init()
        {
            return new SQLConnect(SQLServerName, SQLServerDatabaseName);
        }

        private static Boolean execute(ref SQLConnect _SQLConnect, String SQLStatement, SQLConnect.outputType _outputType, dbTypes _dbType, String ConnectionString)
        {
            //Runs the query based on the type
            Boolean success = true;
            switch (_dbType)
            {
                case dbTypes.Access:
                    success = _SQLConnect.runQuery(ConnectionString, SQLStatement, SQLConnect.connectionType.OleConnection, _outputType);
                    break;
                case dbTypes.BPCS:
                case dbTypes.BPCS_PO:
                case dbTypes.BPCSF:
                    success = _SQLConnect.runQuery(ConnectionString, SQLStatement, SQLConnect.connectionType.OdbcConnection, _outputType);
                    break;
                case dbTypes.SQLServerStatement:
                    success = _SQLConnect.runMSSQLQuery(SQLStatement, _outputType);
                    break;
                case dbTypes.SQLServerStoredProcedure:
                    success = _SQLConnect.runSPQuery(SQLStatement, optvals, _outputType);
                    break;
            }
            if (!success)
            {
                errMess = _SQLConnect.ErrorMessage;
                return false;
            }
            return true;
        }

        private static Boolean doWork(String SQLStatement, SQLConnect.outputType _outputType, dbTypes _dbType, String ConnectionString = "")
        {
            //Initializes the SQLConnect Object so that sql statement can be run
            //creates output base on selected output type
            var SQLObj = init();
            if (!execute(ref SQLObj, SQLStatement, _outputType, _dbType, ConnectionString)) { return false; }
            Results = null;
            ColumnNames = null;
            JSONResults = null;
            switch (_outputType)
            {
                case SQLConnect.outputType.arrayOutput:
                    Results = SQLObj.results;
                    ColumnNames = SQLObj.columnNames;
                    break;
                case SQLConnect.outputType.forRemoteQ:
                    if (SQLObj.DOresults != null)
                    {
                        var oSerializer = new System.Web.Script.Serialization.JavaScriptSerializer();
                        JSONResults = oSerializer.Serialize(SQLObj.DOresults);
                    }
                    break;
            }
            return true;
        }
        #endregion

        
    }
}
