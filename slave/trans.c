/*
 * trans.c
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */
/*
 * transaction actions are defined here.
 */
#include<malloc.h>
#include<sys/time.h>
#include<stdlib.h>
#include<assert.h>
#include<sys/socket.h>
#include"trans.h"
#include"thread_global.h"
#include"data_record.h"
#include"lock_record.h"
#include"mem.h"
#include"proc.h"
#include"snapshot.h"
#include"data_am.h"
#include"transactions.h"
#include"config.h"
#include"thread_main.h"
#include"socket.h"
#include"communicate.h"
#include"server_data.h"
#include"state.h"

//get the transaction id, add proc array and get the snapshot from the master node.
void StartTransactionGetData(void)
{
    int index;
    int i;

    int lindex;
    pthread_t pid;
    Snapshot* snap;
    THREAD* threadinfo;
    TransactionData* td;

    td=(TransactionData*)pthread_getspecific(TransactionDataKey);
    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
    snap=(Snapshot*)pthread_getspecific(SnapshotDataKey);

    int size = 3 + 1 + 1 + MAXPROCS;

    index=threadinfo->index;
    lindex = GetLocalIndex(index);
    pid = pthread_self();
    uint32_t* buf = (uint32_t*)recv_buffer[lindex];

    *(send_buffer[lindex]) = cmd_starttransaction;
    *(send_buffer[lindex]+1) = index;
    *(send_buffer[lindex]+2) = pid;

    if (send(server_socket[lindex], send_buffer[lindex], 3*sizeof(uint64_t), 0) == -1)
        printf("start transaction send error\n");
    if (recv(server_socket[lindex], recv_buffer[lindex], size*sizeof(uint32_t), 0) == -1)
        printf("start transaction recv error\n");

    // get the transaction ID
    td->tid = *(buf+4);

    if(!TransactionIdIsValid(td->tid))
    {
        printf("transaction ID assign error.\n");
        return;
    }

    td->starttime = *(buf+3);
    // get the snapshot
    snap->tcount = *(buf);
    snap->tid_min =*(buf+1);
    snap->tid_max = *(buf+2);

    for (i = 0; i < MAXPROCS; i++)
        snap->tid_array[i] = *(buf+5+i);

    for(i = 0; i < NODENUMMAX; i++)
    {
       td->trans_snap[i] = false;
       td->pure_insert[i] = true;
    }
}

void setPureInsert(int nid)
{
    TransactionData* td;
    td=(TransactionData*)pthread_getspecific(TransactionDataKey);

    td->pure_insert[nid] = false;
}

TimeStampTz GetTransactionStopTimestamp(void)
{
    THREAD* threadinfo;
    int index;
    TransactionData* td;

    td=(TransactionData*)pthread_getspecific(TransactionDataKey);
    // get the pointer to current thread information.
    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

    index=threadinfo->index;
    int lindex;
    lindex = GetLocalIndex(index);

    *(send_buffer[lindex]) = cmd_getendtimestamp;
    if (send(server_socket[lindex], send_buffer[lindex], sizeof(uint64_t), 0) == -1)
       printf("get end time stamp send error\n");
    if (recv(server_socket[lindex], recv_buffer[lindex], sizeof(uint64_t), 0) == -1)
       printf("get end time stamp recv error\n");

    td->stoptime = *(recv_buffer[lindex]);
    return (td->stoptime);
}

void UpdateProcArray()
{
   THREAD* threadinfo;
   int index;
   // get the pointer to current thread information.
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index=threadinfo->index;

   int lindex;
   lindex = GetLocalIndex(index);

   *(send_buffer[lindex]) = cmd_updateprocarray;
   *(send_buffer[lindex]+1) = index;

   if (send(server_socket[lindex], send_buffer[lindex], 2*sizeof(uint64_t), 0) == -1)
      printf("update proc array send error\n");
   if (recv(server_socket[lindex], recv_buffer[lindex], sizeof(uint64_t), 0) == -1)
      printf("update proc array recv error\n");
}

void InitTransactionStructMemAlloc(void)
{
    TransactionData* td;
    THREAD* threadinfo;
    char* memstart;
    Size size;

    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
    memstart=threadinfo->memstart;

    size=sizeof(TransactionData);

    td=(TransactionData*)MemAlloc((void*)memstart,size);

    if(td==NULL)
    {
        printf("memalloc error.\n");
        return;
    }

    pthread_setspecific(TransactionDataKey,td);

    // to set snapshot-data memory.
    InitTransactionSnapshotDataMemAlloc();

    // to set data memory.
    InitDataMemAlloc();
}

/*
 *start a transaction running environment, reset the
 *transaction's information for a new transaction.
 */
void StartTransaction(void)
{
    InitTransactionSnapshotData();

    // to set data memory.
    InitDataMem();

    StartTransactionGetData();
}

void CommitTransaction(void)
{
    GetTransactionStopTimestamp();

    CommitDataRecord();

    UpdateProcArray();

    TransactionMemClean();
}

void AbortTransaction(void)
{
    // process array clean should be after function 'AbortDataRecord'.

    AbortDataRecord();

    // once abort, clean the process array after clean updated-data.
    UpdateProcArray();

    TransactionMemClean();
}

void ReleaseConnect(void)
{
    int i;
    uint64_t release_buffer[1];
    release_buffer[0] = cmd_release_master;
    THREAD* threadinfo;
    int index2;

    int conn;
    uint64_t* sbuffer;

    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
    index2=threadinfo->index;
    int lindex;
    lindex = GetLocalIndex(index2);

    sbuffer=send_buffer[lindex];

    for (i = 0; i < nodenum; i++)
    {
        conn=connect_socket[i][lindex];

        //send data-insert to node "i".
        *(sbuffer) = cmd_release;

        int num = 1;
        Send(conn, sbuffer, num);
    }

    if (send(server_socket[lindex], release_buffer, sizeof(release_buffer), 0) == -1)
        printf("release connect master send error\n");

}

void DataReleaseConnect(void)
{
    int conn;
    uint64_t* sbuffer;

    uint64_t release_buffer[1];

    sbuffer=send_buffer[0];

    conn=connect_socket[nodeid][0];

    // send data-insert to node itself.
    *(sbuffer) = cmd_release;

    int num = 1;
    Send(conn, sbuffer, num);

    release_buffer[0] = cmd_release_master;

    if (send(server_socket[0], release_buffer, sizeof(release_buffer), 0) == -1)
        printf("data release connect master send error\n");
}

/*
void TransactionLoadData(int i)
{
    int j;
    int result;
    int index;
    for (j = 0; j < 20; j++)
    {
      StartTransaction();
      Data_Insert(2, j+1, j+1, nodeid);
      result=PreCommit(&index);
      if(result == 1)
      {
          CommitTransaction();
      }
      else
      {
            //current transaction has to rollback.
            AbortTransaction(index);
      }
    }
    DataReleaseConnect();
    ResetProc();
    ResetMem(0);
}
*/
void TransactionRunSchedule(void* args)
{
    //to run transactions according to args.
    int type;
    int rv;
    terminalArgs* param=(terminalArgs*)args;
    type=param->type;

    if(type==0)
    {
        printf("begin LoadData......\n");
        //LoadData();

        //smallbank
        //LoadBankData();
        switch(benchmarkType)
        {
        case TPCC:
      	  LoadData();
      	  break;
        case SMALLBANK:
      	  LoadBankData();
      	  break;
        default:
      	  printf("benchmark not specified\n");
        }
        DataReleaseConnect();
        ResetMem(0);
        ResetProc();
    }
    else
    {
        printf("ready to execute transactions...\n");

        rv=pthread_barrier_wait(&barrier);
        if(rv != 0 && rv != PTHREAD_BARRIER_SERIAL_THREAD)
        {
            printf("Couldn't wait on barrier\n");
            exit(-1);
        }


        printf("begin execute transactions...\n");
        //executeTransactions(transactionsPerTerminal, param->whse_id, param->dist_id, param->StateInfo);

        //smallbank
        //executeTransactionsBank(transactionsPerTerminal, param->StateInfo);
        switch(benchmarkType)
        {
        case TPCC:
      	  executeTransactions(transactionsPerTerminal, param->whse_id, param->dist_id, param->StateInfo);
      	  break;
        case SMALLBANK:
      	  executeTransactionsBank(transactionsPerTerminal, param->StateInfo);
      	  break;
        default:
      	  printf("benchmark not specified\n");
        }
        ReleaseConnect();
    }
}

void LocalCommitTransaction(int index, TimeStampTz ctime)
{
    LocalCommitDataRecord(index, ctime);

    DataLockRelease(index);
    ResetServerdata(index);
    // transaction have over.
    setTransactionState(index, inactive);
}

void LocalAbortTransaction(int index, int trulynum)
{
    ServerData* data;
    data = serverdata + index;
    // in some status, transaction is failed before the truly manipulate the data record.
    if (data->lock_num == 0)
    {
        setTransactionState(index, aborted);
        ResetServerdata(index);
    }
    else
    {
        // need not broadcast the read transactions because now must be a transaction that not arrive the prepared state.
        LocalAbortDataRecord(index, trulynum);
        // wake up the read transaction
        setTransactionState(index, aborted);
        DataLockRelease(index);
        ResetServerdata(index);
    }
}

/*
 *@return:'1' for success, '-1' for rollback.
 *@input:'index':record the truly done data-record's number when PreCommit failed.
 */
int LocalPreCommit(int* number, int index, TransactionId tid)
{
    LocalDataRecord*start;
    int num,i,result = 1;
    LocalDataRecord* ptr;

    start=(serverdata+index)->datarecord;

    num=(serverdata+index)->data_num;

    // sort the data-operation records.
    LocalDataRecordSort(start, num);

    for(i=0;i<num;i++)
    {
        ptr=start+i;

        switch(ptr->type)
        {
            case DataInsert:
                result=TrulyDataInsert(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value, index, tid);
                break;
            case DataUpdate:
                result=TrulyDataUpdate(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value, index, tid);
                break;
            case DataDelete:
                result=TrulyDataDelete(ptr->table_id, ptr->index, ptr->tuple_id, index, tid);
                break;
            default:
                printf("PreCommit:shouldn't arrive here.\n");
        }
        if(result == -1)
        {
            //return to roll back.
            *number=i;
            return -1;
        }
    }
    setTransactionState(index, prepared);
    return 1;
}

/*
 *@return:'1' for success, '-1' for rollback.
 *@input:'index':record the truly done data-record's number when PreCommit failed.
 */
int PreCommit(void)
{
    TransactionId tid;
    int index;
    char* DataMemStart, *start;
    TransactionData* tdata;
    int num,i;
    DataRecord* ptr;
    THREAD* threadinfo;
    int lindex;

    uint64_t* sbuffer;
    uint64_t* rbuffer;
    int conn;

    DataMemStart=(char*)pthread_getspecific(DataMemKey);
    start=DataMemStart+DataNumSize;
    num=*(int*)DataMemStart;

    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
    index=threadinfo->index;
    lindex = GetLocalIndex(index);

    sbuffer=send_buffer[lindex];
    rbuffer=recv_buffer[lindex];

    tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
    tid=tdata->tid;

    // sort the data-operation records.
    DataRecordSort((DataRecord*)start, num);

    bool t_is_abort = false;
    bool is_abort = false;

    for(i=0;i<num;i++)
    {
        ptr=(DataRecord*)(start+i*sizeof(DataRecord));

        conn=connect_socket[ptr->node_id][lindex];

        if ((tdata->trans_snap[ptr->node_id] == false) && (tdata->pure_insert[ptr->node_id] == false))
        {
            TransSnapshot(ptr->node_id);
        }

        //send data-insert to node "nid".
        *(sbuffer) = cmd_prepare;
        *(sbuffer+1) = tid;

        int num = 2;
        Send(conn, sbuffer, num);

        // response from "nid".
        num = 1;
        Receive(conn, rbuffer, num);

        is_abort = *(rbuffer);

        if (is_abort)
        {
            t_is_abort = true;
        }
    }

    if (t_is_abort)
    {
        return -1;
    }
    else
    {
        return 1;
    }
}

int GetNodeId(int index)
{
   return (index/threadnum);
}

int GetLocalIndex(int index)
{
    return (index%threadnum);
}
