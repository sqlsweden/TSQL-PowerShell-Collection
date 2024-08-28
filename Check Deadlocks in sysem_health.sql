/*
This script analyzes deadlock events captured in the system_health extended events 
session in SQL Server. It retrieves deadlock details, including involved sessions, 
queries, locked objects, and lock types. Optional filtering by start and end date is 
supported. The output helps in identifying the blocker and victim sessions during a 
deadlock for troubleshooting.
*/

-- Parameters
DECLARE @StartDateTime DATETIME2 = NULL;-- Set the start date and time here (NULL fetches all deadlocks)
DECLARE @EndDateTime DATETIME2 = NULL;-- Set the end date and time here (NULL fetches all deadlocks)
DECLARE @FilePath NVARCHAR(4000);

-- Retrieve the default path to the LOG directory dynamically
SELECT @FilePath = LEFT(CONVERT(NVARCHAR(4000), SERVERPROPERTY('ErrorLogFileName')), LEN(CONVERT(NVARCHAR(4000), SERVERPROPERTY('ErrorLogFileName'))) - CHARINDEX('\', REVERSE(CONVERT(NVARCHAR(4000), SERVERPROPERTY('ErrorLogFileName')))) + 1);

-- Append the system_health file pattern to the path
SET @FilePath = @FilePath + 'system_health*.xel';

-- Read and analyze the deadlock events from the system_health session
WITH DeadlockDetails
AS (
	SELECT CAST(XEventFile.event_data AS XML) AS DeadlockXML,
		CAST(XEventFile.event_data AS XML).value('(event/@timestamp)[1]', 'DATETIME2') AS EventTime
	FROM sys.fn_xe_file_target_read_file(@FilePath, -- Use parameterized file path
			NULL, NULL, NULL) AS XEventFile
	WHERE CAST(XEventFile.event_data AS XML).value('(event/@name)[1]', 'NVARCHAR(4000)') = 'xml_deadlock_report'
	),
ProcessDetails
AS (
	SELECT EventTime,
		DeadlockProcess.value('@id', 'NVARCHAR(4000)') AS ProcessID,
		DeadlockProcess.value('@spid', 'INT') AS SessionID,
		CASE 
			WHEN DeadlockProcess.value('@id', 'NVARCHAR(4000)') = VictimList.DeadlockVictim.value('(victim-list/victimProcess/@id)[1]', 'NVARCHAR(4000)')
				THEN 'Victim'
			ELSE 'Blocker'
			END AS ProcessRole,
		ISNULL(DeadlockProcess.value('(inputbuf)[1]', 'NVARCHAR(MAX)'), 'N/A') AS Query,
		ISNULL(ResourceDetails.DeadlockResource.value('@objectname', 'NVARCHAR(4000)'), 'N/A') AS LockedObject,
		ISNULL(ResourceDetails.DeadlockResource.value('@mode', 'NVARCHAR(4000)'), 'N/A') AS LockMode,
		ISNULL(ResourceDetails.DeadlockResource.value('@locktype', 'NVARCHAR(4000)'), ResourceDetails.DeadlockResource.value('@mode', 'NVARCHAR(4000)')) AS LockType,
		ResourceDetails.DeadlockResource.value('@id', 'NVARCHAR(4000)') AS ResourceID
	FROM DeadlockDetails
	CROSS APPLY DeadlockDetails.DeadlockXML.nodes('//deadlock/process-list/process') AS ProcessList(DeadlockProcess)
	CROSS APPLY DeadlockDetails.DeadlockXML.nodes('//deadlock/resource-list/*') AS ResourceDetails(DeadlockResource)
	CROSS APPLY DeadlockDetails.DeadlockXML.nodes('//deadlock') AS VictimList(DeadlockVictim)
	),
RelationMapping
AS (
	SELECT pd1.EventTime,
		pd1.SessionID AS BlockerSession,
		pd2.SessionID AS VictimSession,
		pd2.Query,
		pd2.LockedObject,
		pd2.LockMode,
		pd2.LockType
	FROM ProcessDetails pd1
	JOIN ProcessDetails pd2
		ON pd1.ResourceID = pd2.ResourceID
			AND pd1.ProcessRole = 'Blocker'
			AND pd2.ProcessRole = 'Victim'
	)
SELECT DISTINCT EventTime,
	BlockerSession,
	VictimSession,
	Query,
	LockedObject,
	LockMode,
	LockType
FROM RelationMapping
WHERE (
		@StartDateTime IS NULL
		OR EventTime >= @StartDateTime
		)
	AND (
		@EndDateTime IS NULL
		OR EventTime <= @EndDateTime
		)
ORDER BY EventTime,
	BlockerSession;

-- Resultset for Notes
SELECT 'In this result set, each group of rows represents a separate deadlock event. Here is how to interpret the columns:' AS Note

UNION ALL

SELECT '1. EventTime: The time when the deadlock occurred.'

UNION ALL

SELECT '2. BlockerSession: The session ID of the process that is blocking other processes.'

UNION ALL

SELECT '3. VictimSession: The session ID of the process that was chosen as the victim and terminated by SQL Server to resolve the deadlock.'

UNION ALL

SELECT '4. Query: The SQL query or procedure that was running in the session involved in the deadlock.'

UNION ALL

SELECT '5. LockedObject: The database object (e.g., table or index) that is involved in the deadlock.'

UNION ALL

SELECT '6. LockMode: The type of lock held by the process, such as S (Shared), X (Exclusive), or U (Update).'

UNION ALL

SELECT '7. LockType: The type of lock being requested or held, which might be the same as LockMode in some cases.'

UNION ALL

SELECT 'Each block of rows indicates a deadlock event. The "BlockerSession" indicates the session that is holding a lock that prevents the "VictimSession" from proceeding. SQL Server resolves the deadlock by terminating the "VictimSession" and allowing the "BlockerSession" to continue.' AS Note;