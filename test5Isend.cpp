#include <iostream>
#include "C:\Program Files (x86)\Microsoft SDKs\MPI\Include\mpi.h"
#include <stdint.h>
#include <iostream>
#include <windows.h>
#include <queue>
#include <fstream>
#include "MyTimer.cpp"
using namespace std;
// ans :331152
// cost time :29277756
int myid, numprocs;
const int block = 3000;                           // 分发任务的数量
const int item_N = 50000;                         // 物品数量
const int package_size = 30000;                   // 背包容量大小
const float wait_p = 1.1;                         // 当有多少进程等待时抢占任务
int wait_c = 0;                                   // 等待任务数量
MPI_Datatype tblock_type, tsend_type, tresv_type; // 传送数据的类型定义
typedef struct tblock
{ // 传送节点坐标
    int row, count;
} tblock;
typedef struct tsend
{                                          // 主进程发送结构体
    tblock now;                            // 开始坐标
    int value, weight;                     // 当前的物品属性
    long long last_date[package_size + 1]; // 上一层的数据
} tsend;
typedef struct tresv
{                        // 主进程接收结构体
    tblock now;          // 开始坐标
    long long AU[block]; // 处理完成块
} tresv;
typedef struct nod
{ // 正在处理的块节点记录
    tblock value;
    nod *next;
} nod;
typedef struct status
{ // 当前状态结构体
    tblock next_livelast;
    nod *root;
    nod *last_nod;
} status;
typedef struct content
{ // 相关数据的结构体
    int value[item_N], weight[item_N];
    long long dp[3][package_size + 1];
} content;
MPI_Datatype create_trans()
{ // 创建传输类型
    MPI_Aint base_addr;
    MPI_Aint disp[2] = {0};
    int blocklen[2] = {1, 1};
    MPI_Datatype types[2] = {MPI_INT, MPI_INT};
    tblock s;
    content con;
    MPI_Get_address(&s, &base_addr);
    MPI_Get_address(&s.row, &disp[0]);
    MPI_Get_address(&s.count, &disp[1]);
    for (int i = 0; i < 2; i++)
    {
        disp[i] -= base_addr;
    }
    MPI_Type_create_struct(2, blocklen, disp, types, &tblock_type);
    MPI_Type_commit(&tblock_type);
    MPI_Get_address(&con, &base_addr);
    MPI_Get_address(&con.dp, &disp[0]);

    return tblock_type;
}
MPI_Datatype create_tsend()
{ // 创建传输类型
    MPI_Aint base_addr;
    MPI_Aint disp[4] = {0};
    int blocklen[4] = {1, 1, 1, package_size + 1};
    MPI_Datatype types[4] = {tblock_type, MPI_INT, MPI_INT, MPI_LONG_LONG};
    tsend s;
    MPI_Get_address(&s, &base_addr);
    MPI_Get_address(&s.now, &disp[0]);
    MPI_Get_address(&s.value, &disp[1]);
    MPI_Get_address(&s.weight, &disp[2]);
    MPI_Get_address(&s.last_date, &disp[3]);
    for (int i = 0; i < 4; i++)
    {
        disp[i] -= base_addr;
    }
    MPI_Type_create_struct(4, blocklen, disp, types, &tsend_type);
    MPI_Type_commit(&tsend_type);
    return tsend_type;
}
MPI_Datatype create_tresv()
{ // 创建传输类型
    MPI_Aint base_addr;
    MPI_Aint disp[2] = {0, 0};
    int blocklen[2] = {1, block};
    MPI_Datatype types[2] = {tblock_type, MPI_LONG_LONG};
    tresv s;
    MPI_Get_address(&s, &base_addr);
    MPI_Get_address(&s.now, &disp[0]);
    MPI_Get_address(&s.AU, &disp[1]);
    for (int i = 0; i < 2; i++)
    {
        disp[i] -= base_addr;
    }
    MPI_Type_create_struct(2, blocklen, disp, types, &tresv_type);
    MPI_Type_commit(&tresv_type);
    return tresv_type;
}
bool get_next(tsend &resv, tresv &send)
{                                                         // 从进程获取任务
    MPI_Request req;
    // cout<<send.now.row<<","<<send.now.count<<endl;
    // for(long long a : send.AU)cout<<a<<" ";cout<<endl;

    MPI_Send(&send, 1, tresv_type, 0, 0, MPI_COMM_WORLD); // 将结果发送给主进程
    MPI_Irecv(&resv, 1, tsend_type, 0, 0, MPI_COMM_WORLD, &req);
    MPI_Wait(&req,MPI_STATUSES_IGNORE);
    // cout<<resv.now.row<<","<<resv.now.count<<"_________"<<endl;
    // for(long long a:resv.last_date)cout<<a<<" ";cout<<endl;
    // for(int i=0;i<15;i++){
    //     cout<<resv.last_date[i]<<" ";
    // }cout<<endl;
    return resv.now.row < item_N;
}
void step(tsend &resv, tresv &send)
{ // 从进程处理任务
    int value = resv.value;
    int weight = resv.weight;
    send.now.row = resv.now.row;
    send.now.count = resv.now.count;
    int k=0;
    for (int j = resv.now.count + 1, i = 0; i < block && j <= package_size; j++, i++)
    {
        // cout<<"  "<<j<<"-";
        send.AU[i] = resv.last_date[j];
        if (j >= weight)
        {
            send.AU[i] = max(send.AU[i], resv.last_date[j - weight] + value);
        }
        k++;
    }
}
void delete_node(status &Status, tblock &t)
{ // 主进程删除已经完成的任务
    nod *now = Status.root;
    nod *last = Status.root;
    while (now != NULL)
    {
        if (now->value.row == t.row && now->value.count == t.count)
        {
            // 寻找完成的节点
            if (now == last)
            {
                Status.root = now->next;
            }
            else
                last->next = now->next;
            free(now);
            break;
        }
        else if (now->value.row >= t.row && now->value.count >= t.count)
        {
            // cout<<myid<<" was jump"<<endl;
            break; // 如果不存在跳出，节省时间
        }
        else
        {
            last = now;
            now = now->next;
        }
    }
}
void add_node(status &Status, tblock &t)
{ // 主进程创建任务
    if (Status.root == NULL)
    { // 如果不存在未完成的任务，要创建新任务
        // cout<<"I am first:"<<myid<<endl;
        Status.root = (nod *)malloc(sizeof(nod));
        Status.root->value.row = Status.next_livelast.row;
        Status.root->value.count = Status.next_livelast.count;
        if (Status.next_livelast.count + block >= package_size)
        {
            Status.next_livelast.count = 0;
            Status.next_livelast.row++;
        }
        else
        {
            Status.next_livelast.count += block;
        }
        Status.root->next = NULL;
        Status.last_nod = Status.root;
        t.count = Status.root->value.count;
        t.row = Status.root->value.row;
    }
    else if (Status.next_livelast.row != Status.root->value.row && Status.next_livelast.count + block == Status.root->value.count)
    {
        // 无法往后创建任务
        if (((float)wait_c / (float)numprocs) > wait_p)
        {
            // cout<<"_________________break wait____________"<<endl;
            wait_c = 0;
            t.count = Status.root->value.count;
            t.row = Status.root->value.row;
        }
        else
        {
            // cout<<"_________________all need wait____________"<<endl;
            wait_c++;
            t.count = t.row = -1;
        }
    }
    else
    { // 创建任务
        nod *A = (nod *)malloc(sizeof(nod));
        A->next = NULL;
        A->value.count = Status.next_livelast.count;
        A->value.row = Status.next_livelast.row;
        Status.last_nod->next = A;
        Status.last_nod = A;
        if (Status.next_livelast.count + block >= package_size)
        {
            Status.next_livelast.count = 0;
            Status.next_livelast.row++;
        }
        else
        {
            Status.next_livelast.count += block;
        }
        t.count = A->value.count;
        t.row = A->value.row;
    }
}
void merge_ans(content *cont, tresv &Bresv)
{ // 将结果同步到内存中'
    // cout<<Bresv.now.row<<","<<Bresv.now.count<<"_________"<<endl;
    // // for(long long a:resv.last_date)cout<<a<<" ";cout<<endl;
    // for(int i=0;i<15;i++){
    //     cout<<Bresv.AU[i]<<" ";
    // }cout<<endl;
    if (Bresv.now.row != -1 && Bresv.now.count != -1)
    {
        memcpy(&(cont->dp[(Bresv.now.row) % 3][Bresv.now.count]), &(Bresv.AU), sizeof(long long) * block);
    }
}
void set_last(content *cont, tsend &Bsend)
{ // 将发送需要的数据写入发送消息中
    if (Bsend.now.row != -1 && Bsend.now.count != -1)
    {
        Bsend.value = cont->value[Bsend.now.row];
        Bsend.weight = cont->weight[Bsend.now.row];
        memcpy(Bsend.last_date, cont->dp[(Bsend.now.row + 2) % 3], sizeof(long long) * (package_size + 1));
    }
}
void R_step(content *cont){//主线程分发任务
    // cout<<1<<endl;
    status Status;
    queue<int> Que;
    Status.last_nod = Status.root = NULL;
    Status.next_livelast.count=Status.next_livelast.row = 0;
    tsend *Bsend = (tsend*)malloc(sizeof(tsend)*numprocs);
    cout<<1<<endl;
    tresv Bresv;
    MPI_Status mpi_S;
    int Bsend_size;
    int Size;
    MyTimer time;
    int c = 0;
    MPI_Request *Req = (MPI_Request*)malloc(sizeof(MPI_Request)*numprocs);
    // int last_row = 1;
    cout<<1<<endl;
    while(Status.next_livelast.row<item_N){//只要任务没有分发完成，就循环

        MPI_Recv(&Bresv,1,tresv_type,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&mpi_S);
        // cout<<1<<endl;
        //接受完成数据
        // cout<<Bsend<<endl;
        // time.Start();
        merge_ans(cont,Bresv);
        //将完成的数据写入内存
        delete_node(Status,Bresv.now);
        // time.End();
        // cout<<"push date:"<<time.costTime<<endl;
        //将该节点从正在执行列表中删除
        Que.push(mpi_S.MPI_SOURCE);
        // cout<<2<<endl;
        //将该处理器放入未分配任务的处理器中
        // if(c > numprocs)
        //         MPI_Wait(Req + c%numprocs,MPI_STATUSES_IGNORE);
        // add_node(Status,Bsend[c%numprocs].now);
        // //获取任务
        while(!Que.empty()){//在处理器分配完或不能分发新任务前一直循环
            if(c > numprocs)
                MPI_Wait(Req + (c%numprocs),MPI_STATUSES_IGNORE);
            add_node(Status,Bsend[(c%numprocs)].now);
            if(Bsend[(c%numprocs)].now.count==-1)break;
            //获取任务
            set_last(cont,Bsend[(c%numprocs)]);
            //封装发送消息
            // cout<<2<<endl;
            time.Start();
            MPI_Isend(Bsend + (c%numprocs),1,tsend_type,Que.front(),0,MPI_COMM_WORLD,(Req + (c%numprocs)));
            time.End();
            // cout<<3<<endl;
            c++;
            // cout<<"Bsend cost time:"<<time.costTime<<" ,C:"<<c<<endl;
            //发送消息
            Que.pop();
            //将该处理器从未分配中取出
            
        }
    }
    // Send cost time:32.7596
    // Bsend cost time:26.0969
    // Isend cost time:0.017402
    for(int i=1 ; i < numprocs ; i++){//将所有处理器进程退出
        MPI_Recv(&Bresv,1,tresv_type,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&mpi_S);
        Bsend[c%numprocs].now.row = Status.next_livelast.row;
        Bsend[c%numprocs].now.count = Status.next_livelast.count;
        MPI_Isend(Bsend + c%numprocs,1,tsend_type,mpi_S.MPI_SOURCE,0,MPI_COMM_WORLD,(Req + c%numprocs));
        c++;
    }
}
int main(int argc, char *argv[])
{
    int namelen;
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &myid);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Get_processor_name(processor_name, &namelen);
    create_trans();
    create_tresv();
    create_tsend();
    MPI_Aint size_state = myid == 0 ? sizeof(status) : 0;
    MPI_Aint size_cont = myid == 0 ? sizeof(content) : 0;
    status *rstate;
    int disp_state = sizeof(status);
    int disp_cont = sizeof(content);
    content *cont = (content *)malloc(sizeof(content));
    if (myid == 0)
    { // 通过0进程读取文件，并且搭建内存共享
        memset(cont->dp, 0, sizeof(cont->dp));
        ifstream V, W;
        V.open("in_V.txt");
        W.open("in_w.txt");
        int a, b;
        for (int i = 0; i < item_N; i++)
        {
            V >> a;
            W >> b;
            cont->value[i] = a;
            cont->weight[i] = b;
        }
    }
    if (myid == 0)
    {
        cout << "开始监听" << endl;
        MyTimer time;
        time.Start();
        R_step(cont);
        time.End();
        cout << "ans :" << cont->dp[(item_N ) % 3][package_size - 1] << endl;
        // cout << "ans :" << cont->dp[(item_N + 1 ) % 3][package_size] << endl;
        // cout << "ans :" << cont->dp[(item_N + 2 ) % 3][package_size] << endl;
        cout << "cost time :" << time.costTime << endl;
    }
    else
    {
        tsend Bresv;
        tresv Bsend;
        Bsend.now.row = Bsend.now.count = -1;
        Bresv.now.row = Bresv.now.count = -1;
        while (get_next(Bresv, Bsend))
        {
            step(Bresv, Bsend);
        }
    }
    MPI_Finalize();
    return 0;
}
