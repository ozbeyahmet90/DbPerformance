SET NOCOUNT ON;

DECLARE @objectid        INT;
DECLARE @indexid         INT;
DECLARE @partitioncount  BIGINT;
DECLARE @schemaname      NVARCHAR(130);
DECLARE @objectname      NVARCHAR(130);
DECLARE @indexname       NVARCHAR(130);
DECLARE @partitionnum    BIGINT;
DECLARE @partitions      BIGINT;
DECLARE @frag            FLOAT;
DECLARE @command         NVARCHAR(4000);
DECLARE @dbid            SMALLINT;

SET @dbid = DB_ID('YourDatabaseName');
SELECT @dbid;

SELECT [object_id] AS objectid,
       index_id AS indexid,
       partition_number AS partitionnum,
       avg_fragmentation_in_percent AS frag,
       page_count
       
       INTO #work_to_do
FROM   sys.dm_db_index_physical_stats(@dbid, NULL, NULL, NULL, 'limited')
WHERE  avg_fragmentation_in_percent > 10.0 -- Allow limited fragmentation
       AND index_id > 0 -- Ignore heaps
       AND page_count > 25; -- Ignore small tables

DECLARE partitions  CURSOR  
FOR
    SELECT objectid,
           indexid,
           partitionnum,
           frag     
    FROM   #work_to_do
;

-- Open the cursor.

OPEN partitions;

-- Loop through the partitions.

WHILE (1 = 1)
BEGIN
    FETCH NEXT
    
    FROM partitions
    
    INTO @objectid, @indexid, @partitionnum, @frag;
    
    IF @@FETCH_STATUS < 0
        BREAK;
    
    SELECT @objectname = QUOTENAME(o.name),
           @schemaname = QUOTENAME(s.name)
    FROM   sys.objects AS o
           JOIN sys.schemas AS s
                ON  s.schema_id = o.schema_id
    WHERE  o.object_id = @objectid;
    
    SELECT @indexname = QUOTENAME(NAME)
    FROM   sys.indexes
    WHERE  OBJECT_ID = @objectid
           AND index_id = @indexid;
    
    SELECT @partitioncount = COUNT(*)
    FROM   sys.partitions
    WHERE  OBJECT_ID = @objectid
           AND index_id = @indexid;
    
    -- 30 is an æarbitrary decision point at which to switch between reorganizing and rebuilding.
    
    IF @frag < 30.0
        SET @command = N'ALTER INDEX ' + @indexname + N' ON ' + @schemaname +
            N'.' + @objectname + N' REORGANIZE';
    
    IF @frag >= 30.0
        SET @command = N'ALTER INDEX ' + @indexname + N' ON ' + @schemaname +
            N'.' + @objectname + N' REBUILD';

    PRINT N'Starting Execute : ' + @command;
    
    EXEC (@command);
    
    PRINT N'Executed: ' + @command;
END

-- Close and deallocate the cursor.

CLOSE partitions;

DEALLOCATE partitions;

-- Drop the temporary table.

DROP TABLE #work_to_do;

GO

EXEC sp_updatestats

GO