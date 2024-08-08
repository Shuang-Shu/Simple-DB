
package simpledb.storage;

import simpledb.common.Database;
import simpledb.transaction.TransactionId;
import simpledb.common.Debug;
import simpledb.common.Permissions;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.lang.reflect.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/*
LogFile implements the recovery subsystem of SimpleDb.  This class is
able to write different log records as needed, but it is the
responsibility of the caller to ensure that write ahead logging and
two-phase locking discipline are followed.  <p>

<u> Locking note: </u>
<p>

Many of the methods here are synchronized (to prevent concurrent log
writes from happening); many of the methods in BufferPool are also
synchronized (for similar reasons.)  Problem is that BufferPool writes
log records (on page flushed) and the log file flushes BufferPool
pages (on checkpoints and recovery.)  This can lead to deadlock.  For
that reason, any LogFile operation that needs to access the BufferPool
must not be declared synchronized and must begin with a block like:

这里的许多方法都是同步的(以防止并发日志写入发生);BufferPool中的许多方法也是同步的(出于类似的原因)。
问题是BufferPool写日志记录(在页面刷新时)，日志文件刷新BufferPool页面(在检查点和恢复时)。这可能导致死锁。
因此，任何需要访问BufferPool的LogFile操作都不能被声明为synchronized，并且必须像这样以块开头:

<p>
<pre>
    synchronized (Database.getBufferPool()) {
       synchronized (this) {

       ..

       }
    }
</pre>
*/

/**
 * <p>
 * The format of the log file is as follows:
 * 
 * <ul>
 * 
 * <li>The first long integer of the file represents the offset of the
 * last written checkpoint, or -1 if there are no checkpoints
 * 文件的第一个长整数表示最后一个写入检查点的偏移量，如果没有检查点，则为-1
 * 
 * <li>All additional data in the log consists of log records. Log
 * records are variable length.
 * 日志中的所有附加数据都由日志记录组成。日志记录是可变长度的。
 * 
 * <li>Each log record begins with an integer type and a long integer
 * transaction id.
 * 每个日志记录以一个整数类型和一个长整数事务id开始。
 * 
 * <li>Each log record ends with a long integer file offset representing
 * the position in the log file where the record began.
 * 每个日志记录以一个整数类型和一个长整数事务id开始。
 * 
 * <li>There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and
 * CHECKPOINT
 * 有五种记录类型:ABORT、COMMIT、UPDATE、BEGIN和CHECKPOINT
 * 
 * <li>ABORT, COMMIT, and BEGIN records contain no additional data
 * ABORT、COMMIT和BEGIN记录不包含任何其他数据
 * 
 * <li>UPDATE RECORDS consist of two entries, a before image and an
 * after image. These images are serialized Page objects, and can be
 * accessed with the LogFile.readPageData() and LogFile.writePageData()
 * methods. See LogFile.print() for an example.
 * 
 * UPDATE RECORDS由两个条目组成，一个前镜像和一个后镜像。这些镜像是序列化的Page对象，
 * 可以通过LogFile.readPageData()和LogFile.writePageData()方法访问。参见LogFile.print()的示例。
 * 
 * <li>CHECKPOINT records consist of active transactions at the time
 * the checkpoint was taken and their first log record on disk. The format
 * of the record is an integer count of the number of transactions, as well
 * as a long integer transaction id and a long integer first record offset
 * for each active transaction.
 * 
 * 检查点记录由检查点被获取时的活动事务和它们在磁盘上的第一个日志记录组成。
 * 记录的格式是事务数的整数计数，以及每个活动事务的长整数事务id和长整数第一个记录偏移量
 * 
 * </ul>
 */
public class LogFile {

    final File logFile;
    private RandomAccessFile raf;
    Boolean recoveryUndecided; // no call to recover() and no append to log 没有调用recover()，也没有追加到日志

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;

    static final long NO_CHECKPOINT_ID = -1;

    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;
    final static byte[] HEADER_BUF = new byte[INT_SIZE + LONG_SIZE];
    final static AtomicBoolean rollbackOnce = new AtomicBoolean();

    long currentOffset = -1;// protected by this
    // int pageSize;
    int totalEntitiesNumber = 0; // for PatchTest //protected by this

    // an in-memory data structure to keep track of the offset of each uncommitted
    // transaction
    final Map<TransactionId, Long> aliveTidOffsetMap = new HashMap<>();

    /**
     * Constructor.
     * Initialize and back the log file with the specified file.
     * We're not sure yet whether the caller is creating a brand new DB,
     * in which case we should ignore the log file, or whether the caller
     * will eventually want to recover (after populating the Catalog).
     * So we make this decision lazily: if someone calls recover(), then
     * do it, while if someone starts adding log file entries, then first
     * throw out the initial log file contents.
     * 
     * 用指定的文件初始化并返回日志文件。我们还不确定调用方是否正在创建一个全新的DB,在这种情况下,我们应该忽略日志文件,或者调用者是否最终想要恢复(在填充目录之后)。因此,我们推迟了这个决定:如果有人调用恢复(),那么就去做吧,如果有人开始添加日志文件条目,那么首先抛出初始日志文件内容
     * 
     * @param f The log file's name
     */
    public LogFile(File f) throws IOException {
        this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;

        // install shutdown hook to force cleanup on close
        // Runtime.getRuntime().addShutdownHook(new Thread() {
        // public void run() { shutdown(); }
        // });

        // XXX WARNING -- there is nothing that verifies that the specified
        // log file actually corresponds to the current catalog.
        // This could cause problems since we log tableids, which may or
        // may not match tableids in the current catalog.
    }

    // we're about to append a log record. if we weren't sure whether the
    // DB wants to do recovery, we're sure now -- it didn't. So truncate
    // the log.
    // 我们将附加一个日志记录。
    // 如果我们不确定DB是否想要恢复,我们现在确定--它不想恢复。所以对日志进行截断
    void preAppend() throws IOException {
        totalEntitiesNumber++;
        if (recoveryUndecided) {
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            // 截断文件，并写入 写入检查点的偏移量-1，即阶段后不再有写入检查点
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    public synchronized int getTotalEntitiesNumber() {
        return totalEntitiesNumber;
    }

    /**
     * Write an abort record to the log for the specified tid, force
     * the log to disk, and perform a rollback
     * 将终止记录写入指定tid的日志，将日志强制写入磁盘，然后执行回滚
     * 
     * @param tid The aborting transaction.
     */
    public void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback
        // 在继续之前必须有缓冲池锁，因为这将调用回滚

        synchronized (Database.getBufferPool()) {

            synchronized (this) {
                preAppend();
                // Debug.log("ABORT");
                // should we verify that this is a live transaction?
                // 我们是否应该验证这是一个活动事务?

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
                // 必须在这里做这个，因为回滚只对活动事务有效(需要tidToFirstLogRecord)
                rollback(tid);
                // 向日志写入值
                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                force();
                aliveTidOffsetMap.remove(tid);
            }
        }
    }

    /**
     * Write a commit record to disk for the specified tid,
     * and force the log to disk.
     * 
     * 将指定tid的提交记录写入磁盘，并强制日志写入磁盘。
     * 
     * @param tid The committing transaction.
     */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        Debug.log("COMMIT " + tid.getId());
        // should we verify that this is a live transaction?

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        aliveTidOffsetMap.remove(tid);
    }

    /**
     * Write an UPDATE record to disk for the specified tid and page
     * (with provided before and after images.)
     * 
     * 将指定tid和page的UPDATE记录写入磁盘(提供前后映像)
     * 
     * @param tid    The transaction performing the write
     * @param before The before image of the page
     * @param after  The after image of the page
     * 
     * @see Page#getBeforeImage
     */
    public synchronized void logWrite(TransactionId tid, Page before,
            Page after)
            throws IOException {
        Debug.log("WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /*
         * update record conists of
         * 
         * record type
         * transaction id
         * before page data (see writePageData)
         * after page data
         * start offset
         * 
         * 更新记录由
         * 记录类型
         * 事务id
         * 在页面数据之前(参见writePageData)
         * 后页面数据
         * 起始偏移量
         */
        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());
        // 写入数据
        writePageData(raf, before);
        writePageData(raf, after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("WRITE OFFSET = " + currentOffset);
    }

    void writePageData(RandomAccessFile raf, Page p) throws IOException {
        PageId pid = p.getId();
        int[] pageInfo = pid.serialize();

        // page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data
        /**
         * 页面数据是:
         * 页面类名
         * id类名
         * id类字节
         * id类数据
         * 页面类字节
         * 页面类数据
         */

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int j : pageInfo) {
            raf.writeInt(j);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        // Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +
        // pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object[] idArgs = new Object[numIdArgs];
            for (int i = 0; i < numIdArgs; i++) {
                idArgs[i] = raf.readInt();
            }
            pid = (PageId) idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); // read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page) pageConsts[0].newInstance(pageArgs);

            // Debug.log("READ PAGE OF TYPE " + pageClassName + ", table = " +
            // newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException | InvocationTargetException | IllegalAccessException
                | InstantiationException e) {
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /**
     * Write a BEGIN record for the specified transaction
     * 为指定的事务写入BEGIN记录
     * 
     * @param tid The transaction that is beginning
     * 
     */
    public synchronized void logXactionBegin(TransactionId tid)
            throws IOException {
        Debug.log("BEGIN");
        if (aliveTidOffsetMap.containsKey(tid)) {
            System.err.print("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }
        preAppend();
        raf.writeInt(BEGIN_RECORD); // 事务开始
        raf.writeLong(tid.getId()); // 事务id
        raf.writeLong(currentOffset); // 偏移量
        aliveTidOffsetMap.put(tid, currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("BEGIN OFFSET = " + currentOffset);
    }

    /** Checkpoint the log and write a checkpoint record. */
    // 对日志进行检查点并写入检查点记录
    public void logCheckpoint() throws IOException {
        // make sure we have buffer pool lock before proceeding
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                // Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCheckpointOffset;
                Set<Long> rawTidSet = Set
                        .copyOf(aliveTidOffsetMap.keySet().stream().map(tid -> tid.getId())
                                .collect(Collectors.toList()));
                Iterator<Long> rawTidIter = rawTidSet.iterator(); // iterate on all alive transactions
                force(); // persist all data
                Database.getBufferPool().flushAllPages();
                startCheckpointOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1); // no tid , but leave space for convenience

                // write list of outstanding transactions
                // 写入未完成事务的清单
                raf.writeInt(rawTidSet.size());
                while (rawTidIter.hasNext()) {
                    Long rawTid = rawTidIter.next();
                    Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + rawTid);
                    raf.writeLong(rawTid);
                    // Debug.log("WRITING CHECKPOINT TRANSACTION OFFSET: " +
                    raf.writeLong(aliveTidOffsetMap.get(new TransactionId(rawTid)));
                }

                // once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                raf.writeLong(startCheckpointOffset);
                raf.seek(0);
                raf.writeLong(startCheckpointOffset); // update the log header
            }
        }
        logTruncate();
    }

    /**
     * Truncate any unneeded portion of the log to reduce its space
     * consumption
     */
    // 截断日志中任何不需要的部分，以减少其空间消耗
    public synchronized void logTruncate() throws IOException {
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong(); // checkpoint offset

        long minLogRecord = cpLoc;

        if (cpLoc != -1L) {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            @SuppressWarnings("unused")
            long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt(); // number of uncommited transaction

            for (int i = 0; i < numOutstanding; i++) {
                @SuppressWarnings("unused")
                long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }

        // we can truncate everything before minLogRecord
        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);

        // have to rewrite log records since offsets are different after truncation
        while (true) {
            try {
                int type = raf.readInt();
                long recordTid = raf.readLong();
                long newStart = logNew.getFilePointer();

                Debug.log("NEW START = " + newStart);

                logNew.writeInt(type);
                logNew.writeLong(recordTid);

                switch (type) {
                    case UPDATE_RECORD:
                        Page before = readPageData(raf);
                        Page after = readPageData(raf);

                        writePageData(logNew, before);
                        writePageData(logNew, after);
                        break;
                    case CHECKPOINT_RECORD:
                        int numXactions = raf.readInt();
                        logNew.writeInt(numXactions);
                        while (numXactions-- > 0) {
                            long xid = raf.readLong();
                            long xoffset = raf.readLong();
                            logNew.writeLong(xid);
                            logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                        }
                        break;
                    case BEGIN_RECORD:
                        aliveTidOffsetMap.put(new TransactionId(recordTid), newStart);
                        break;
                }
                // all xactions finish with a pointer
                logNew.writeLong(newStart);
                raf.readLong();
            } catch (EOFException e) {
                System.out.println("err: " + e.getMessage());
                break;
            }
        }

        Debug.log("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: "
                + (raf.length() - minLogRecord));

        raf.close();
        try {
            Files.move(newFile.toPath(), logFile.toPath(), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();
        // print();
    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state. To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     * 
     * 回滚指定的事务，将其更新的任何页面的状态设置为更新前的状态。
     * 为了保持事务语义，不应该对已经提交的事务调用该函数
     * (尽管该方法可能不会强制执行该函数)。
     * 
     * rollback is also a transaction
     * 
     * @param tid The transaction to rollback
     */
    public void rollback(TransactionId tid)
            throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                preAppend();
                // some code goes here
                // 找到所有与中止事务相关的更新记录
                raf.seek(0);
                Long checkPoint = raf.readLong();
                Set<Long> uncommittedRawTidSet = new HashSet<>();
                Long offset = Long.valueOf(-1);
                if (checkPoint == -1) {
                    // no checkpoint is found, only rollback current transaction
                    offset = aliveTidOffsetMap.get(tid);
                    uncommittedRawTidSet.add(tid.getId());
                } else {
                    if (rollbackOnce.compareAndSet(false, true)) {
                        // get all uncommitted transactions
                        raf.seek(checkPoint);
                        raf.read(HEADER_BUF); // skip trx number & tid
                        int numTransactions = raf.readInt();
                        while (numTransactions-- > 0) {
                            Long uncommittedRawTid = raf.readLong();
                            uncommittedRawTidSet.add(uncommittedRawTid);
                            Long recordOffset = raf.readLong();
                            if (uncommittedRawTid == tid.getId()) {// FIRST LOG RECORD
                                offset = recordOffset;
                            }
                        }
                        raf.readLong();
                    } else {
                        // rollback of checkpoint should be done only once
                        offset = raf.getFilePointer();
                    }
                }
                raf.seek(offset);
                while (true) {
                    try {
                        int entityType = raf.readInt();
                        long entityTid = raf.readLong();

                        if (!uncommittedRawTidSet.contains(entityTid)) {
                            break;
                        }

                        switch (entityType) {
                            case BEGIN_RECORD, ABORT_RECORD, COMMIT_RECORD: // no data, just skip
                                raf.readLong();
                                break;
                            case CHECKPOINT_RECORD: // no data, just skip
                                int numTransactions = raf.readInt();
                                while (numTransactions-- > 0) {
                                    raf.readLong();// tid
                                    raf.readLong();// FIRST LOG RECORD
                                }
                                raf.readLong();
                                break;
                            case UPDATE_RECORD:
                                // rollback the uncommitted transaction
                                Page before = readPageData(raf);
                                readPageData(raf);
                                // physically reset the page to the before image
                                Database.getBufferPool().setPage(tid, before.getId(), Permissions.READ_WRITE, before);
                                Database.getBufferPool().flushPages(tid);
                                break;
                        }
                    } catch (EOFException e) {
                        break;
                    }
                }
            }
        }
    }

    /**
     * Shutdown the logging system, writing out whatever state
     * is necessary so that start up can happen quickly (without
     * extensive recovery.)
     */
    public synchronized void shutdown() {
        try {
            logCheckpoint(); // simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                recoveryUndecided = false;
                // some code goes here
                TransactionId recoverId = new TransactionId();
                raf.seek(0);
                Long checkPointOffset = raf.readLong();
                Set<Long> uncommittedTransactions = new HashSet<>();
                long startOffset = 8;// 扫描开始的offset，如果没有checkpoint则是8；否则是checkpoint后的一个第一个字节位置
                if (checkPointOffset != -1) {
                    // 此时有checkpoint
                    raf.seek(checkPointOffset);
                    raf.readInt();
                    raf.readLong();
                    int numTransactions = raf.readInt();
                    while (numTransactions-- > 0) {
                        long tid = raf.readLong();
                        raf.readLong();
                        uncommittedTransactions.add(tid);
                    }
                    raf.readLong();
                    startOffset = raf.getFilePointer();
                }
                // 从startOffset开始第一次扫描，检查所有committedTransactions和uncommittedTransactions
                raf.seek(startOffset);
                while (true) {
                    try {
                        int recordType = raf.readInt();
                        long recordTid = raf.readLong();
                        switch (recordType) {
                            case ABORT_RECORD:
                                // 对于中止事务，将其放入committedTransactions
                                if (uncommittedTransactions.contains(recordTid))
                                    uncommittedTransactions.remove(recordTid);
                                raf.readLong();
                                break;
                            case COMMIT_RECORD:
                                // 对于提交事务，将其放入committedTransactions
                                if (uncommittedTransactions.contains(recordTid))
                                    uncommittedTransactions.remove(recordTid);
                                raf.readLong();
                                break;
                            case UPDATE_RECORD:
                                // 第一次扫描时跳过
                                readPageData(raf);
                                readPageData(raf);
                                raf.readLong();
                                break;
                            case BEGIN_RECORD:
                                // 对于新的tid，将其放入uncommittedTransactions
                                uncommittedTransactions.add(recordTid);
                                raf.readLong();
                                break;
                        }
                    } catch (EOFException e) {
                        break;
                    }
                }
                // 第二次扫描，处理所有页面的redo和undo
                raf.seek(startOffset);
                while (true) {
                    try {
                        int recordType = raf.readInt();
                        long recordTid = raf.readLong();
                        switch (recordType) {
                            case BEGIN_RECORD, COMMIT_RECORD, ABORT_RECORD: // not data, just skip
                                raf.readLong();
                                break;
                            case UPDATE_RECORD:
                                // 第二次扫描时进行处理
                                Page before = readPageData(raf);
                                Page after = readPageData(raf);

                                PageId pid = before.getId();

                                if (uncommittedTransactions.contains(recordTid)) {
                                    // 此时进行undo
                                    Database.getBufferPool().setPage(recoverId, pid, Permissions.READ_WRITE, before);
                                } else {
                                    // 此时进行redo
                                    if (recordType == COMMIT_RECORD) {
                                        Database.getBufferPool().setPage(recoverId, pid, Permissions.READ_WRITE, after);
                                    } else {
                                        Database.getBufferPool().setPage(recoverId, pid, Permissions.READ_WRITE,
                                                before);
                                    }
                                }
                                Database.getBufferPool().flushPages(recoverId);
                                raf.readLong();
                                break;
                        }
                    } catch (EOFException e) {
                        break;
                    }
                }
                Database.getBufferPool().transactionComplete(recoverId);
            }
        }
    }

    /** Print out a human readable represenation of the log */
    public void print() throws IOException {
        long curOffset = raf.getFilePointer();

        raf.seek(0);

        System.out.println("0: checkpoint record at offset " + raf.readLong());

        while (true) {
            try {
                System.out.println("-----RECORD BEGIN-----");
                int cpType = raf.readInt();
                long cpTid = raf.readLong();

                System.out.println((raf.getFilePointer() - (INT_SIZE + LONG_SIZE)) + ": RECORD TYPE " + cpType);
                System.out.println((raf.getFilePointer() - LONG_SIZE) + ": TID " + cpTid);

                switch (cpType) {
                    case BEGIN_RECORD:
                        System.out.println(" (BEGIN)");
                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                        break;
                    case ABORT_RECORD:
                        System.out.println(" (ABORT)");
                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                        break;
                    case COMMIT_RECORD:
                        System.out.println(" (COMMIT)");
                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                        break;

                    case CHECKPOINT_RECORD:
                        System.out.println(" (CHECKPOINT)");
                        int numTransactions = raf.readInt();
                        System.out.println((raf.getFilePointer() - INT_SIZE) + ": NUMBER OF OUTSTANDING RECORDS: "
                                + numTransactions);

                        while (numTransactions-- > 0) {
                            long tid = raf.readLong();
                            long firstRecord = raf.readLong();
                            System.out.println((raf.getFilePointer() - (LONG_SIZE + LONG_SIZE)) + ": TID: " + tid);
                            System.out
                                    .println((raf.getFilePointer() - LONG_SIZE) + ": FIRST LOG RECORD: " + firstRecord);
                        }
                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                        break;
                    case UPDATE_RECORD:
                        System.out.println(" (UPDATE)");

                        long start = raf.getFilePointer();
                        Page before = readPageData(raf);

                        long middle = raf.getFilePointer();
                        Page after = readPageData(raf);

                        System.out.println(start + ": before image table id " + before.getId().getTableId());
                        System.out.println(
                                (start + INT_SIZE) + ": before image page number " + before.getId().getPageNumber());
                        System.out.println((start + INT_SIZE) + " TO " + (middle - INT_SIZE) + ": page data");

                        System.out.println(middle + ": after image table id " + after.getId().getTableId());
                        System.out.println(
                                (middle + INT_SIZE) + ": after image page number " + after.getId().getPageNumber());
                        System.out.println((middle + INT_SIZE) + " TO " + (raf.getFilePointer()) + ": page data");

                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                        break;

                }
                System.out.println("-----RECORD END-----\n");

            } catch (EOFException e) {
                // e.printStackTrace();
                break;
            }
        }

        // Return the file pointer to its original position
        raf.seek(curOffset);
    }

    public synchronized void force() throws IOException {
        raf.getChannel().force(true);
    }
}
