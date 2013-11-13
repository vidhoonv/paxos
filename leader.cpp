//logging
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

#include<stdio.h>
#include <pthread.h>
#include<algorithm>
#include "pax_types.h"

using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

// Define static logger variable
LoggerPtr LeaderLogger(Logger::getLogger("leader"));

#define MAX_SET_SIZE 100
#define TALKER leader_comm.comm_fd[TALKER_INDEX]
#define LISTENER leader_comm.comm_fd[LISTENER_INDEX]

#define INCREASE_INDEX 0
#define DECREASE_INDEX 1

int ACCEPTOR_PORT_LIST[MAX_ACCEPTORS] = {3000,3002,3004};//,3006,3008,3010,3012,3014,3016,3018};
int LEADER_PORT_LIST[MAX_LEADERS] = {4000,4002};//,4003};
int REPLICA_PORT_LIST[MAX_REPLICAS] = {2000,2002};
//int COMMANDER_PORT_LIST[MAX_COMMANDERS] = {5000,5001,5002,5003,5004,5005,5006,5007,5008,5009,5010,5011,5012,5013,5014,5015,5016,5017,5018,5019,5020,5021,5022,5023,5024,5025,5026,5027,5028,5029,5030,5031,5032,5033,5034,5035,5036,5037,5038,5039,5040,5041,5042,5043,5044,5045,5046,5047,5048,5049,5050,5051,5052,5053,5054,5055,5056,5057,5058,5059};
//int SCOUT_PORT_LIST[MAX_SCOUTS] = {6000,6001,6002,6003,6004,6005,6006,6007,6008,6009,6010,6011,6012,6013,6014,6015,6016,6017,6018,6019,6020,6021,6022,6023,6024,6025,6026,6027,6028,6029,6030,6031,6032,6033,6034,6035,6036,6037,6038,6039,6040,6041,6042,6043,6044,6045,6046,6047,6048,6049,6060,6051,6052,6053,6054,6055,6056,6057,6058,6059};

void* commander(void*);
void* scout(void*);


//create a mapping such that acc_pvals[slot_number][0/1] = (command/highest ballot number) with highest ballot 
#define POST_PROCESS_PVALS(ACC_MAP1,ACC_MAP2,STR) \
					 temp = strtok_r(STR,DELIMITER_SEC,&tok1); \
					while(temp) \
					{ \
						printf("here %s\n",temp);\
						/*BALLOT NUMBER (temp1,temp2)*/ \
						recv_ballot.bnum = atoi(temp); \
						recv_ballot.leader_id = atoi(strtok_r(NULL,DELIMITER_SEC,&tok1)); \
						/*slot number */ \
						slot_number = atoi(strtok_r(NULL,DELIMITER_SEC,&tok1)); \
						command = atoi(strtok_r(NULL,DELIMITER_SEC,&tok1)); \
						if(ballot_compare(recv_ballot,acc_pvals_hballot[slot_number]) > 0) \
						{ \
							printf("in ballot compare \n");\
							ACC_MAP1[slot_number] = command; \
							ACC_MAP2[slot_number] = recv_ballot; \
						} \
						temp = strtok_r(NULL,DELIMITER_SEC,&tok1); \
					}


				//manipulate the leaders pending proposals according the acc_pvals_map
#define MANIPULATE_LEADER_PLIST(ACC_CMD_MAP,PROP_LIST,DEC_LIST) \
					for(i=0;i<MAX_SLOTS;i++) \
					{ \
						if(ACC_CMD_MAP[i] != -1 && DEC_LIST.command[i] != -2) \
							PROP_LIST.command[i] = ACC_CMD_MAP[i]; \
					} 

#define CREATE_SCOUT_THREAD \
	LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << "creating scout thread for ballot (" << leader_state.ballot.bnum << "," << leader_state.ballot.leader_id <<  ") !\n");	\
	scout_create_args[count_scouts].parent_id = my_pid;	\
	scout_create_args[count_scouts].my_pid = count_scouts+my_pid*MAX_SCOUTS_PER_LEADER; 	\
	scout_create_args[count_scouts].my_ballot = leader_state.ballot; 	\
	rc = pthread_create(&scout_thread[count_scouts], NULL, scout, (void *)&scout_create_args[count_scouts]);	\
	count_scouts++;

enum LEADER_STATUS
{
	LEADER_INACTIVE=0,	
	LEADER_ACTIVE	
};
struct STATE_LEADER
{
	struct BALLOT_NUMBER ballot;
	enum LEADER_STATUS lstatus;
	struct PROPOSAL plist;
	struct PROPOSAL dlist;
	unsigned int preemption_timeout; //microseconds
	float timeout_factor[2]; //0 - increase by factor; 1 - decrease by factor
};

int ballot_compare(struct BALLOT_NUMBER other_ballot,struct BALLOT_NUMBER my_ballot)
{
	if(other_ballot.bnum-my_ballot.bnum == 0)
	{
		return (other_ballot.leader_id-my_ballot.leader_id);	
	}
	else
	{
		return(other_ballot.bnum-my_ballot.bnum); 
	}
}
void ballot_copy(struct BALLOT_NUMBER other_ballot,struct BALLOT_NUMBER *my_ballot)
{
	my_ballot->bnum = other_ballot.bnum;
	my_ballot->leader_id = other_ballot.leader_id;
}

bool configure_leader(int my_pid,struct COMM_DATA *comm_leader)
{
	
	int listener_fd,port,talker_fd;
	socklen_t listener_len;
	int yes=1;
	int i;
	struct sockaddr_in talker_addr, listener_addr, *process_addr_in;
	//listen setup
	if ((listener_fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0){
		perror("listener socket ");
        	return false;
    	}

	if (setsockopt(listener_fd,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(int)) == -1) {
		perror("setsockopt");
		return false;
	}
    	listener_addr.sin_family = AF_INET;
   	listener_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	listener_addr.sin_port = htons(LEADER_PORT_LIST[my_pid]); 

	if (bind(listener_fd, (struct sockaddr *) &listener_addr, sizeof(listener_addr)) < 0)
	{
	        perror("listener bind ");
	        close(listener_fd);
	        return false;
	}
    	listener_len = sizeof(listener_addr);

	if (getsockname(listener_fd, (struct sockaddr *)&listener_addr, &listener_len) < 0)
    	{
        	perror("listener getsockname ");
        	close(listener_fd);
        	return false;
    	}
    	printf("leader: listener using port %d\n", ntohs(listener_addr.sin_port));

	//talker setup
	if (( talker_fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0){
			perror("talker socket ");
       			return false;
	}
	printf("leader: talker_fd = %d\n",talker_fd);

	talker_addr.sin_family = AF_INET;
	talker_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    	talker_addr.sin_port = htons(0);  // pick any free port

	if (bind(talker_fd, (struct sockaddr *) &talker_addr, sizeof(talker_addr)) < 0)
    	{
        perror("leader: talker bind ");
        close(talker_fd);
        return false;
    	}   

	comm_leader->comm_fd[LISTENER_INDEX] = listener_fd;
	comm_leader->comm_fd[TALKER_INDEX] = talker_fd;

return true;
}

int get_current_active_leader(int lstatus[])
{
	int i;

	for(i=0;i<MAX_LEADERS;i++)
	{
		if(lstatus[i] == 1)
			return i;
	}
	return -1;
}
bool broadcast_leaders(int my_pid,int talker_fd,char send_buff[],struct sockaddr leader_addr[],socklen_t leader_addr_len[])
{
	int i,ret;
	for(i=0;i<MAX_LEADERS;i++)
	{
/*//NETWORK PARTITION TEST CASE (simulated by msg loss)
		if(my_pid==0 && i==1 || my_pid==1 && i==0)
			continue;
*/
/*
//LEADER FAILURE DETECTION TEST CASE
		if(my_pid == 0 && i==2)
			exit(-1);
*/		if(i!=my_pid)
		{
		ret = sendto(talker_fd, send_buff, strlen(send_buff), 0, 
      			(struct sockaddr *)&leader_addr[i], leader_addr_len[i]);
			
		if (ret < 0)
     		{
      			perror("sendto ");
		        close(talker_fd);
      			return false;
     		}
		}
		
	}
return true;
}
bool broadcast_replicas(int my_pid,int talker_fd,char send_buff[],struct sockaddr replica_addr[],socklen_t replica_addr_len[])
{
	int i,ret;
	for(i=0;i<MAX_REPLICAS;i++)
	{
/* LEADER FAILURE DETECTION TEST CASE
		if(i==1 && my_pid==0)
			exit(-1);
*/
		ret = sendto(talker_fd, send_buff, strlen(send_buff), 0, 
      			(struct sockaddr *)&replica_addr[i], replica_addr_len[i]);
			
		if (ret < 0)
     		{
      			perror("sendto ");
		        close(talker_fd);
      			return false;
     		}
		
	}
return true;
}

bool check_lease_status(time_t lts)
{//returns true when lease is critical (about to expire)
	time_t cur_time;
	time(&cur_time); 
	printf("\n\n clock compare %.f",difftime(cur_time,lts));;
	if(difftime(cur_time,lts) >= LEASE_PERIOD-LEASE_OFFSET)
		return true;
	else
		return false;
	
}
int main(int argc,char **argv)
{
	struct COMM_DATA leader_comm;
	struct STATE_LEADER leader_state;
	int my_pid;

//comm common
	struct sockaddr_storage temp_paddr;
	socklen_t temp_paddr_len;

//comm related variables
	struct hostent *hp;
	char hostname[64];

//comm listening variables
	fd_set readfds;
	int maxfd;
	char recv_buff[BUFSIZE];
	int nread=0;	
//comm sending variables
	char send_buff[BUFSIZE];
	struct sockaddr_in *leader_addr_in[MAX_LEADERS],*replica_addr_in[MAX_REPLICAS];
	struct sockaddr leader_addr[MAX_LEADERS],replica_addr[MAX_REPLICAS];
	socklen_t leader_addr_len[MAX_LEADERS],replica_addr_len[MAX_REPLICAS];
//misc
	int i,ret=0,recv_pid,rc=0;
	bool command_found = false;
	char *data,*temp,*tok,*tok1;
	char *ballot_str,*pvals_str,*pstr;
	char buff_copy[BUFSIZE];
	int slot_number,command;
	struct BALLOT_NUMBER recv_ballot,temp_ballot;

//threads
	pthread_t commander_thread[MAX_COMMANDERS_PER_LEADER],scout_thread[MAX_SCOUTS_PER_LEADER];
	struct COMMANDER_THREAD_ARG comm_create_args[MAX_COMMANDERS_PER_LEADER];
	int count_commanders=0, count_scouts=0;
	struct SCOUT_THREAD_ARG scout_create_args[MAX_SCOUTS_PER_LEADER];

//pval set from acceptors
	struct BALLOT_NUMBER acc_pvals_hballot[MAX_SLOTS]; //// this contains acc_pvals_hballot[slot_number]= current highest ballot
	int acc_pvals_command[MAX_SLOTS] = {-1}; //for time being this is int to into - might changed based on type of 'command'
					// this contains acc_pvals_command[slot_number]= command with highest ballot number from acceptor 

//leader status
	int leader_status[MAX_LEADERS];
	int active_leader;
	int ping_timeout = 5;
	int read_timeout = 10;
	int update_timeout = 3;
//timeout
	struct timeval tv,tread,tupdate;
	struct timeval *tptr=NULL;
//command execution
	int current_batch =1;
//read command list
	int read_commands[MAX_SLOTS];
	int read_command_counter=0;
	bool read_issued = false;
	int uptodate_replicas[MAX_REPLICAS];
	int replica_status[MAX_REPLICAS];
	int latest_replica_pid = -1;
	bool noupdate = false;
	int read_issued_till = -1;

//lease timer
	bool lease_critical = false; 
	time_t lease_timestamp;
	
	//check runtime arguments
	if(argc!=2)
	{
		printf("Usage: ./leader <leader_id>\n");
		return -1;
	}
	my_pid=atoi(argv[1]);
//configure logger
 DOMConfigurator::configure("leader_log_config.xml");
	//initialization
	//leader_state.plist.current_length = 0;
	leader_state.ballot.bnum = 0;
	leader_state.ballot.leader_id = my_pid;
	leader_state.lstatus = LEADER_INACTIVE; 

	std::fill(acc_pvals_command, acc_pvals_command + MAX_SLOTS, -1);
	std::fill(leader_state.plist.command,leader_state.plist.command+MAX_SLOTS,-1);
	std::fill(leader_state.dlist.command,leader_state.dlist.command+MAX_SLOTS,-1);
	std::fill(read_commands,read_commands+MAX_SLOTS,-1);

	std::fill(leader_status, leader_status + MAX_LEADERS, 1); //All leaders are alive
	std::fill(uptodate_replicas,  uptodate_replicas + MAX_REPLICAS, 0); //All replicas are not uptodate initially
	std::fill(replica_status,  replica_status + MAX_REPLICAS, 0); //All replicas status not alive initially

	leader_state.preemption_timeout = 2000000;
	leader_state.timeout_factor[INCREASE_INDEX] = 1.5;
	leader_state.timeout_factor[DECREASE_INDEX] = 1000000;

	tread.tv_sec = 0;
	tread.tv_usec = 0;	

	tupdate.tv_sec = update_timeout;
	tupdate.tv_usec = 0;	

	tptr = &tupdate; //intially providing update timeout

	LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << "timeout factors " << leader_state.timeout_factor[INCREASE_INDEX] << "," << leader_state.timeout_factor[DECREASE_INDEX]<<"\n");


	//hostname configuration
	gethostname(hostname, sizeof(hostname));
	hp = gethostbyname(hostname);
	if (hp == NULL) 
	{ 
		LOG4CXX_TRACE(LeaderLogger,hostname << " : unknown host\n");
		//printf("\n%s: unknown host.\n", hostname); 
		return 0; 
	} 
	
	//setup leader addresses
	for(i=0;i<MAX_LEADERS;i++)
	{
		leader_addr_in[i] = (struct sockaddr_in *)&(leader_addr[i]);
		leader_addr_in[i]->sin_family = AF_INET;
		memcpy(&leader_addr_in[i]->sin_addr, hp->h_addr, hp->h_length); 
		leader_addr_in[i]->sin_port  = htons(LEADER_PORT_LIST[i]);  
		leader_addr_len[i] = sizeof(leader_addr[i]);
	}
	//setup replica addresses
	for(i=0;i<MAX_REPLICAS;i++)
	{
		replica_addr_in[i] = (struct sockaddr_in *)&(replica_addr[i]);
		replica_addr_in[i]->sin_family = AF_INET;
		memcpy(&replica_addr_in[i]->sin_addr, hp->h_addr, hp->h_length); 
		replica_addr_in[i]->sin_port  = htons(REPLICA_PORT_LIST[i]);  
		replica_addr_len[i] = sizeof(replica_addr[i]);
	}
	//configure leader talker and listener ports	
	//setup the leader 
	if(configure_leader(my_pid,&leader_comm))
	{
		LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " configured successfully\n");
		//printf("Leader id: %d configured successfully\n",my_pid);
	}
	else
	{
		LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " config error \n");
		//printf("Error in config of leader id: %d\n",my_pid);
		return -1;
	}

	active_leader = get_current_active_leader(leader_status);

	if(active_leader == my_pid)
	{
#if DEBUG==1
	LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << "creating scout thread for ballot (" << leader_state.ballot.bnum << "," << leader_state.ballot.leader_id <<  ") !\n");
	//printf("Leader id: %d creating scout thread for ballot (%d,%d)!\n",my_pid,leader_state.ballot.bnum,leader_state.ballot.leader_id);
#endif
	//create a new scout thread
	scout_create_args[count_scouts].parent_id = my_pid;
	scout_create_args[count_scouts].my_pid = count_scouts+my_pid*MAX_SCOUTS_PER_LEADER;
	scout_create_args[count_scouts].my_ballot = leader_state.ballot; 
	rc = pthread_create(&scout_thread[count_scouts], NULL, scout, (void *)&scout_create_args[count_scouts]);
	count_scouts++;
	
	//store lease timestamp
	time(&lease_timestamp);

		//send alive message to all leader
		printf("\nSending alive message to all leaders\n");
		strcpy(send_buff,"ALIVE"); 	
		strcat(send_buff,DELIMITER);
		sprintf(send_buff,"%s%d",send_buff,my_pid);	
		strcat(send_buff,DELIMITER);
		
		
			
		if (broadcast_leaders(my_pid,TALKER,send_buff,leader_addr,leader_addr_len))
		{
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " alive broadcasted \n");
							printf("alive broadcasted at leader %d\n",my_pid);
							
#endif
							
		}
		else
		{
			LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " alive broadcast failed \n");			
			printf("broadcast of alive failed at leader %d\n",my_pid);	
		}

	}
	else
	{
		tv.tv_sec = ping_timeout;
		tv.tv_usec = 0;	

	}

	while(1)
	{
		maxfd = LISTENER+1;
		FD_ZERO(&readfds); 
		FD_SET(LISTENER, &readfds);
		if(my_pid == active_leader)
		{
			ret = select(maxfd, &readfds, NULL, NULL,tptr);  //blocks forever till it receives a message
		}
		else
			ret = select(maxfd, &readfds, NULL, NULL, &tv); 

		if(ret <0)
	   	{ 
			LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " select error\n");
	     		//printf("\nLeader id: %d Select error\n",my_pid);   
	     		return -1;
	   	} 
		if(ret == 0)
		{
			printf("Time out while waiting for msgs %d read_cmd_counter %d\n",latest_replica_pid,read_command_counter);

			if(active_leader == my_pid && leader_state.lstatus == LEADER_ACTIVE)
			{

				if(latest_replica_pid == -1)
				{
					//timeout waiting for commit

					//this can happen only if either all replicas failed or there was no update command in the current batch
					//since only all but 1 replicas can fail in the system, we only consider the case that there were no update commands

#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " no updates in current batch "<< current_batch <<" \n");
							printf("Leader id: %d no updates in current batch %d\n",my_pid,current_batch);
							
#endif
					//no way to determine live replica at first - so proceed by trail and error basis
					latest_replica_pid = 0;
					//set boolean flag 
					noupdate = true;

				}
				else
				{
					if(read_command_counter >=0 && read_commands[read_command_counter] == -1)
					{

						//there was no read commands that were sent and leader timed out
						// in this case leader did not time out on read commit 
						// so just wait for update commands
						if(check_lease_status(lease_timestamp) == true)
						{
							lease_critical = true;
							CREATE_SCOUT_THREAD;
							time(&lease_timestamp);
						}
							tupdate.tv_sec = update_timeout;
							tupdate.tv_usec = 0;
							tptr = &tupdate;
						
						continue;
					}
				//timeout waiting for read commit
				
				//do read again to some other replica

					if(read_command_counter != 0)
					{
						replica_status[latest_replica_pid] = 0;
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " detected replica "<< latest_replica_pid <<" Dead \n");
							printf("detected replica %d dead\n",my_pid,latest_replica_pid);
							
#endif
					}
					if(current_batch == 1)
					{
						latest_replica_pid++;
						if(latest_replica_pid == MAX_REPLICAS)
						{
							printf("!!finding uptodate replica failed\n");
							return -1;
						}
					}
					else
					{
						for(i=0;i<MAX_REPLICAS;i++)
						{

							if(replica_status[i] == 1 && uptodate_replicas[i] == 1)
								break;
						}
						if(i== MAX_REPLICAS)
						{
							printf("!!finding uptodate replica failed\n");
							return -1;
						}	
						latest_replica_pid = i;
					}
				}
					if(check_lease_status(lease_timestamp) == true)
					{
						lease_critical = true;
						CREATE_SCOUT_THREAD;
						time(&lease_timestamp);
						//current_batch++;
						continue;
					}

					//send read commands to another replica
					//issue read commands accumulated during this batch to this replica
					i=read_command_counter;
					if(read_commands[i] != -1)
					{
						
						//there is an accumulated read command
						//sending data in the format
						//DECISION:COMMANDER_ID:-1:COMMAND
						//printf("here\n");
						strcpy(send_buff,"DECISION");
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,-1);
						strcat(send_buff,DELIMITER);

						while(read_commands[i] != -1)
						{

#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " sending read command "<< i<< " \n");
							printf("sending read command %d \n",i);
							
#endif
					
							sprintf(send_buff,"%s%d",send_buff,read_commands[i]);
							strcat(send_buff,DELIMITER_SEC);
					
							i++;
						}
						strcat(send_buff,DELIMITER);
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " read command string "<< send_buff<< " \n");
							printf("sending read command %s \n",send_buff);
							
#endif	

						ret = sendto(TALKER, send_buff, strlen(send_buff), 0, 
      								(struct sockaddr *)&replica_addr[latest_replica_pid], replica_addr_len[latest_replica_pid]);
			
						if (ret < 0)
     						{
      							perror("sendto ");
						        close(TALKER);
      							//return false;
     						}
					read_issued_till = i;
					//read timeout
					tread.tv_sec = read_timeout;
					tread.tv_usec = 0; 
					tptr = &tread;

					read_issued = true;

					}
					else
					{
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " no read commands"<< " \n");
							printf("no read commands \n");
							
#endif	

//no read commands to execute after this batch
						tupdate.tv_sec = update_timeout;
						tupdate.tv_usec = 0;
						tptr = &tupdate;
						if(noupdate == false)
						{
							current_batch++;
						}
						else
							noupdate = false; //do not increment current batch if it did not have any update commands
						read_issued = false;
						i=(current_batch-1)*BATCH_SIZE;
						while(leader_state.dlist.command[i] != -1)
						{

						//DECISION BELONGS TO CURRENT BATCH - BROADCAST TO REPLICAS

						//sending data in the format
						//DECISION:COMMANDER_ID:SLOT_NUMBER:COMMAND
						//printf("here\n");
						strcpy(send_buff,"DECISION");
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,i);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,leader_state.dlist.command[i]);
						strcat(send_buff,DELIMITER);

						if(broadcast_replicas(my_pid,TALKER,send_buff,replica_addr,replica_addr_len))
						{
#if DEBUG == 1
						LOG4CXX_TRACE(LeaderLogger,"LEader id: " << my_pid << " decision broadcasted \n");
						//printf("decision broadcasted at LEADER %d\n",my_pid);
							
#endif
	
						//remove entry from proposal list 	
						//leader_state.plist.command[slot_number] = -1;
						leader_state.dlist.command[slot_number] = -2;
						
						}
						else
						{
						LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " decision broadcast failed \n");			
						//printf("broadcast of decision failed at LEADER %d\n",my_pid);	
						}

						i++;
					
						}

					}				

			}
			else
			{

			if(active_leader != my_pid)
			{
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " leader "<<active_leader <<" is dead \n");
							printf("leader %d is dead\n",active_leader);
							
#endif
				leader_status[active_leader] = 0;
				active_leader = get_current_active_leader(leader_status);
			}
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " new active leader "<<active_leader <<" \n");
							printf("new active leader %d \n",active_leader);
							
#endif

			if(active_leader == my_pid)
			{
#if DEBUG==1
				LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << "creating scout thread for ballot (" << leader_state.ballot.bnum << "," << leader_state.ballot.leader_id <<  ") !\n");
				//printf("Leader id: %d creating scout thread for ballot (%d,%d)!\n",my_pid,leader_state.ballot.bnum,leader_state.ballot.leader_id);
#endif
				//create a new scout thread
				scout_create_args[count_scouts].parent_id = my_pid;
				scout_create_args[count_scouts].my_pid = count_scouts+my_pid*MAX_SCOUTS_PER_LEADER;
				scout_create_args[count_scouts].my_ballot = leader_state.ballot; 
				rc = pthread_create(&scout_thread[count_scouts], NULL, scout, (void *)&scout_create_args[count_scouts]);
				count_scouts++;

				//send ping message to active leader
				printf("\nSending alive message to all leader\n");
				strcpy(send_buff,"ALIVE"); 	
				strcat(send_buff,DELIMITER);
				sprintf(send_buff,"%s%d",send_buff,my_pid);	
				strcat(send_buff,DELIMITER);
		
		
			
				if (broadcast_leaders(my_pid,TALKER,send_buff,leader_addr,leader_addr_len))
				{
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " alive broadcasted \n");
							printf("alive broadcasted at leader %d\n",my_pid);
							
#endif
							
				}
				else
				{
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " alive broadcast failed \n");			
							printf("broadcast of alive failed at leader %d\n",my_pid);	
				}

				//ping timeout
				tupdate.tv_sec = ping_timeout;
				tupdate.tv_usec = 0; 
				tptr = &tupdate;
				continue;
			}
			else
			{
		
				tv.tv_sec = ping_timeout;
				tv.tv_usec = 0;	
				continue;

			}
			}			
		}
		if(FD_ISSET (LISTENER, &readfds))
		{
			temp_paddr_len = sizeof(temp_paddr);
			nread = recvfrom (LISTENER, recv_buff, BUFSIZE, 0, 
               	       			(struct sockaddr *)&temp_paddr, &temp_paddr_len); 
		
		 	if (nread < 0)
		       	{
		        	perror("recvfrom ");
            			close(LISTENER);
            			return -1;
        		}		
			recv_buff[nread] = 0;
  			printf("Leader id: %d received: %s\n",my_pid, recv_buff);

			strcpy(buff_copy,recv_buff);			
			data = strtok_r(buff_copy,DELIMITER,&tok);

//retrive recv_pid
				recv_pid = atoi(strtok_r(NULL,DELIMITER,&tok));
#if DEBUG==1
				LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " received: " << recv_buff << " from: " << recv_pid << "\n");
				printf("Leader id: %d recved msg from %d\n",my_pid,recv_pid);
#endif		
			if(strcmp(data,"PROPOSE") == 0)
			{
				//received from REPLICA
				//expects data in the format
				//PROPOSE:REPLICA_ID:SLOTNUMBER:COMMAND:

				//retrive slot num
				slot_number = atoi(strtok_r(NULL,DELIMITER,&tok));
			
				//retrive command
				command = atoi(strtok_r(NULL,DELIMITER,&tok));
				
				if(slot_number == -1)
				{
					//received a proposal for read only command - store and continue

					for(i=0;i<MAX_SLOTS;i++)
					{
						if(read_commands[i] == command)
							break;
						if(read_commands[i] == -1)
						{
							read_commands[i] = command;
							break;
						}
					}
					continue;
				}
				if(leader_state.plist.command[slot_number] == -1)
				{
					//new proposal
/*TESTCASE - LEADER 0 FAILS AFTER RECEIVING PROPOSAL FOR SLOT 1 (FAILURE DETECTION OF LEADERS)
					if(my_pid ==0 && slot_number==1)
						exit(-1);
*/
#if DEBUG==1
					LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << "no previous proposal for slot number -- added to plist!\n");
					//printf("Leader id: %d no previous proposal for slot number -- added to plist!\n",my_pid);
#endif

					//add new proposal to plist
					leader_state.plist.command[slot_number] = command;
					
					if(leader_state.lstatus == LEADER_ACTIVE && lease_critical == false)
					{
#if DEBUG==1
						LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << "is active now - creating commander thread" << count_commanders+my_pid*MAX_COMMANDERS_PER_LEADER << "\n");
						//printf("Leader id: %d  is active now - creating commander thread %d!\n",my_pid,count_commanders);
#endif
						//create a new commander thread
						comm_create_args[count_commanders].parent_id = my_pid;
						comm_create_args[count_commanders].my_pid = count_commanders+my_pid*MAX_COMMANDERS_PER_LEADER;
						comm_create_args[count_commanders].my_pval.ballot = leader_state.ballot; 
						comm_create_args[count_commanders].my_pval.slot_number = slot_number;
						comm_create_args[count_commanders].my_pval.command = command; 
						rc = pthread_create(&commander_thread[count_commanders], NULL, commander, (void *)&comm_create_args[count_commanders]);
#if DEBUG==1
						LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << "created commander thread" << count_commanders+my_pid*MAX_COMMANDERS_PER_LEADER << "\n");
						//printf("Leader id: %d created commander thread %d!\n",my_pid,count_commanders);
#endif
						count_commanders++;


					}
				}
				else
				{
					//old proposal
#if DEBUG==1
					LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << "already proposed for this slot number\n");
					//printf("Leader id: %d already proposed for this slot number!\n",my_pid);
#endif
				}

			}
			else if(strcmp(data,"ADOPTED") == 0)
			{
				//received from SCOUT
				//expects data in the format
				//ADOPTED:SCOUT_ID:BALLOT:PVALS:

				ballot_str = strtok_r(NULL,DELIMITER,&tok);
				pvals_str = strtok_r(NULL,DELIMITER,&tok);

				recv_ballot.bnum = atoi(strtok_r(ballot_str,DELIMITER_SEC,&tok1));
				recv_ballot.leader_id = atoi(strtok_r(NULL,DELIMITER_SEC,&tok1));

				if(ballot_compare(recv_ballot,leader_state.ballot) == 0)
				{

					//leased
					lease_critical = false;
					//see if some reading are pending in current batch and increment current batch
					
					if(read_issued == false && latest_replica_pid != -1)
					{
						//this scout was sent to renew lease before sending read commands
						//so send read commands
//issue read commands accumulated during this batch to this replica
					i=read_command_counter;
					if(read_commands[i] != -1)
					{
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " constructing decision for read commands"<< " \n");
							
							
#endif	
						//there is an accumulated read command
						//sending data in the format
						//DECISION:COMMANDER_ID:-1:COMMAND
						//printf("here\n");
						strcpy(send_buff,"DECISION");
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,-1);
						strcat(send_buff,DELIMITER);

					while(read_commands[i] != -1)
					{
					
						sprintf(send_buff,"%s%d",send_buff,read_commands[i]);
						strcat(send_buff,DELIMITER_SEC);
					
						i++;
					}
						strcat(send_buff,DELIMITER);

#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " sending read commands to replica " << recv_pid << " \n");
							printf("sending read commands at leader %d to replica %d\n",my_pid,recv_pid);
							
#endif
						ret = sendto(TALKER, send_buff, strlen(send_buff), 0, 
      								(struct sockaddr *)&replica_addr[latest_replica_pid], replica_addr_len[latest_replica_pid]);
			
						if (ret < 0)
     						{
      							perror("sendto ");
						        close(TALKER);
      							//return false;
     						}
					read_issued_till = i;
					//read timeout
					tread.tv_sec = read_timeout;
					tread.tv_usec = 0; 
					tptr = &tread;

					read_issued = true;
					}
					else
					{
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " no read commands"<< " \n");
							printf("no read commands \n");
							
#endif	
						//no read commands to execute after this batch
						tupdate.tv_sec = update_timeout;
						tupdate.tv_usec = 0;
						tptr = &tupdate;
						if(noupdate == false)
						{
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " batch updated"<< " \n");
							
							
#endif	
							current_batch++;
						}
						else
							noupdate = false; //do not increment current batch if it did not have any update commands
						i=(current_batch-1)*BATCH_SIZE;
						read_issued = false;
						while(leader_state.dlist.command[i] != -1)
						{

						//DECISION BELONGS TO CURRENT BATCH - BROADCAST TO REPLICAS

						//sending data in the format
						//DECISION:COMMANDER_ID:SLOT_NUMBER:COMMAND
						//printf("here\n");
						strcpy(send_buff,"DECISION");
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,i);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,leader_state.dlist.command[i]);
						strcat(send_buff,DELIMITER);

						if(broadcast_replicas(my_pid,TALKER,send_buff,replica_addr,replica_addr_len))
						{
#if DEBUG == 1
						LOG4CXX_TRACE(LeaderLogger,"LEader id: " << my_pid << " decision broadcasted \n");
						//printf("decision broadcasted at LEADER %d\n",my_pid);
							
#endif
	
						//remove entry from proposal list 	
						//leader_state.plist.command[slot_number] = -1;
						leader_state.dlist.command[slot_number] = -2;
						
						}
						else
						{
						LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " decision broadcast failed \n");			
						//printf("broadcast of decision failed at LEADER %d\n",my_pid);	
						}

						i++;
					
						}
						
					}



					}
					
		
											
					if(pvals_str)
					{
						// fetch PVALS and insert into PVAL struct
						//create a mapping such that acc_pvals[slot_number] = command with highest ballot 
						POST_PROCESS_PVALS(acc_pvals_command,acc_pvals_hballot,pvals_str);
						//manipulate the leaders pending proposals according the acc_pvals_map
						MANIPULATE_LEADER_PLIST(acc_pvals_command,leader_state.plist,leader_state.dlist);
					}
					//create commander thread for each pending proposal with leader
					for(i=0;i<MAX_SLOTS;i++)
					{
	
						if(leader_state.plist.command[i] == -1 || leader_state.dlist.command[i] == -2)
							continue;

printf("!!!!here\n");
#if DEBUG==1
						LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " creating commander thread "  <<count_commanders+my_pid*MAX_COMMANDERS_PER_LEADER << " for proposal <(" << leader_state.ballot.bnum <<","<< leader_state.ballot.leader_id << "):" << i << ":"<< leader_state.plist.command[i] <<  ">!\n");
						//printf("Leader id: %d creating commander thread for proposal <(%d,%d):%d:%d>!\n",my_pid,leader_state.ballot.bnum,leader_state.ballot.leader_id,i,leader_state.plist.command[i]);
#endif
						//create a new commander thread
						comm_create_args[count_commanders].parent_id = my_pid;
						comm_create_args[count_commanders].my_pid = count_commanders+my_pid*MAX_COMMANDERS_PER_LEADER;
						comm_create_args[count_commanders].my_pval.ballot = leader_state.ballot; 
						comm_create_args[count_commanders].my_pval.slot_number = i;
						comm_create_args[count_commanders].my_pval.command = leader_state.plist.command[i]; 
						rc = pthread_create(&commander_thread[count_commanders], NULL, commander, (void *)&comm_create_args[count_commanders]);
						count_commanders++;

					}
					//change status to active
					leader_state.lstatus = LEADER_ACTIVE;
					//DECREASE TIMEOUT
					if(leader_state.preemption_timeout>=leader_state.timeout_factor[DECREASE_INDEX])
						leader_state.preemption_timeout = leader_state.preemption_timeout-leader_state.timeout_factor[DECREASE_INDEX];
#if DEBUG==1
						LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " decreased timeout " << leader_state.preemption_timeout << "\n");
#endif
				}
				else
				{
					LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << "received ballot ("<< recv_ballot.bnum << ":" << recv_ballot.leader_id << ")\n");
					//printf("Leader id: %d received ballot (%d:%d) which must be old - ignoring\n",my_pid,recv_ballot.bnum,recv_ballot.leader_id);

				}	
			}
			else if(strcmp(data,"PREEMPTED") == 0)
			{
				//received from SCOUT or COMMANDER
				//expects data in the format
				//PREEMPTED:SENDER_ID:BALLOT:
#if DEBUG==1
						LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " preempted " << "\n");
#endif	
				ballot_str = strtok_r(NULL,DELIMITER,&tok);

				//retrive components of the recv_ballot
				recv_ballot.bnum = atoi(strtok_r(ballot_str,DELIMITER_SEC,&tok1));
				recv_ballot.leader_id = atoi(strtok_r(NULL,DELIMITER_SEC,&tok1));

				if(ballot_compare(recv_ballot,leader_state.ballot) > 0)
				{
					
					leader_state.lstatus = LEADER_INACTIVE;
#if DEBUG==1
						LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " increased timeout " << leader_state.preemption_timeout*leader_state.timeout_factor[INCREASE_INDEX] << " factor:" << leader_state.timeout_factor[INCREASE_INDEX] << "\n");
#endif	
					//INCREASE TIMEOUT
					leader_state.preemption_timeout = leader_state.preemption_timeout*leader_state.timeout_factor[INCREASE_INDEX];

					if(leader_state.preemption_timeout > 0)
					{
						if(usleep(leader_state.preemption_timeout) == 0)
						{
#if DEBUG==1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << "timeout over: " << leader_state.preemption_timeout << "\n");
#endif	
						}
					}
					leader_state.ballot.bnum = recv_ballot.bnum + 1;

					//create a new scout thread for the new ballot 					
#if DEBUG==1
						LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << "creating scout thread " << count_scouts+my_pid*MAX_SCOUTS_PER_LEADER <<" for ballot ("<< leader_state.ballot.bnum << ":" << leader_state.ballot.leader_id << ")\n");
						//printf("Leader id: %d creating scout thread for ballot (%d,%d)!\n",my_pid,leader_state.ballot.bnum,leader_state.ballot.leader_id);
#endif
						//create a new scout thread
						scout_create_args[count_scouts].parent_id = my_pid;
						scout_create_args[count_scouts].my_pid = count_scouts+my_pid*MAX_SCOUTS_PER_LEADER;
						scout_create_args[count_scouts].my_ballot = leader_state.ballot; 
						rc = pthread_create(&scout_thread[count_scouts], NULL, scout, (void *)&scout_create_args[count_scouts]);
						count_scouts++;
						time(&lease_timestamp);
				}
			}
			else if(strcmp(data,"DECISION") == 0)
			{
				//recved from commander 
				//expects data in the format
				//DECISION:COMMANDER_ID:SLOT_NUMBER:COMMAND:
				

				//retrive decision components
#if DEBUG == 1
				LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " received decision \n");
#endif

				slot_number = atoi(strtok_r(NULL,DELIMITER,&tok));
#if DEBUG == 1
				LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " slot "  << slot_number << "\n");
#endif
				command = atoi(strtok_r(NULL,DELIMITER,&tok));
#if DEBUG == 1
				LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " command "  << command << "\n");
#endif

#if DEBUG == 1
				LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " current_batch "  << current_batch << "\n");
#endif		
/*//TEST CASE LEADER CRASH
if(my_pid == 0 && slot_number == 1)
exit(-1);		
*/				if(slot_number < current_batch*BATCH_SIZE)
				{
					//DECISION BELONGS TO CURRENT BATCH - BROADCAST TO REPLICAS

						//sending data in the format
						//DECISION:COMMANDER_ID:SLOT_NUMBER:COMMAND
						//printf("here\n");
						strcpy(send_buff,"DECISION");
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,slot_number);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,command);
						strcat(send_buff,DELIMITER);

					if(broadcast_replicas(my_pid,TALKER,send_buff,replica_addr,replica_addr_len))
					{
#if DEBUG == 1
						LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " decision broadcasted \n");
						//printf("decision broadcasted at leader %d\n",my_pid);
							
#endif
	
						//remove entry from proposal list 	
						leader_state.dlist.command[slot_number] = -2;
						
					}
					else
					{
						LOG4CXX_TRACE(LeaderLogger,"leader id: " << my_pid << " decision broadcast failed \n");			
						//printf("broadcast of decision failed at leader %d\n",my_pid);	
					}
				}
				else
				{
					//DECISION DOES NOT BELONG TO CURRENT BATCH - STORE IN DECISION LIST
					leader_state.dlist.command[slot_number] = command;

				}

#if DEBUG == 1
				LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " removed entry for slot "  << slot_number << "\n");
#endif
			}
			else if(strcmp(data,"ALIVE") == 0)
			{
				
				if(active_leader != recv_pid)
				{
					leader_status[active_leader] = -1;					
					active_leader = recv_pid;
				}	
				//send ping message to active leader
				printf("\nSending ping message to active leader\n");
				strcpy(send_buff,"PING"); 	
				strcat(send_buff,DELIMITER);
				sprintf(send_buff,"%s%d",send_buff,my_pid);	
				strcat(send_buff,DELIMITER);
				
				rc = sendto(TALKER, send_buff, strlen(send_buff), 0, 
      					(struct sockaddr *)&leader_addr[active_leader], leader_addr_len[active_leader]);
			
				if (rc < 0)
     				{
      					perror("sendto ");
		    		    close(TALKER);
      					//return false;
     				}			
				tv.tv_sec = ping_timeout;
				tv.tv_usec = 0;	
	
				
			}
			else if(strcmp(data,"PING") == 0)
			{
			//send ping message to active leader
				if(active_leader != my_pid)
				{
				leader_status[active_leader] = 0;
				active_leader = my_pid;


				#if DEBUG==1
				LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << "creating scout thread for ballot (" << leader_state.ballot.bnum << "," << leader_state.ballot.leader_id <<  ") !\n");
				//printf("Leader id: %d creating scout thread for ballot (%d,%d)!\n",my_pid,leader_state.ballot.bnum,leader_state.ballot.leader_id);
#endif
				//create a new scout thread
				scout_create_args[count_scouts].parent_id = my_pid;
				scout_create_args[count_scouts].my_pid = count_scouts+my_pid*MAX_SCOUTS_PER_LEADER;
				scout_create_args[count_scouts].my_ballot = leader_state.ballot; 
				rc = pthread_create(&scout_thread[count_scouts], NULL, scout, (void *)&scout_create_args[count_scouts]);
				count_scouts++;

				}
				printf("responding to ping message by active leader\n");
				strcpy(send_buff,"ALIVE"); 	
				strcat(send_buff,DELIMITER);
				sprintf(send_buff,"%s%d",send_buff,my_pid);	
				strcat(send_buff,DELIMITER);
				
				rc = sendto(TALKER, send_buff, strlen(send_buff), 0, 
      					(struct sockaddr *)&leader_addr[recv_pid], leader_addr_len[recv_pid]);
			
				if (rc < 0)
     				{
      					perror("sendto ");
		    		    close(TALKER);
      					//return false;
     				}			
			}
			else if(strcmp(data,"COMMIT") == 0)
			{

				//expects data in the format
				//COMMIT:<REPLICA_ID>:<CURRENT_SLOT_NUMBER>

				slot_number = atoi(strtok_r(NULL,DELIMITER,&tok));
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " current_batch"<<current_batch<<" \n");
							printf("got commit current_batch %d \n",current_batch);
							
#endif	
				if(slot_number == current_batch*BATCH_SIZE)
				{
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " got commit - proceeding to try reads"<< " \n");
							printf("got commit - proceeding to try reads \n");
							
#endif	
					//recv_pid has pid of latest replica that has executed all update commands in a batch
					uptodate_replicas[recv_pid]=1;
					replica_status[recv_pid]=1;

					latest_replica_pid = recv_pid;
					if(lease_critical == true) //lease was found critical and is being renewed
						continue;
					if(read_issued == false)
					{

						//check lease and renew if it is critical
						if(check_lease_status(lease_timestamp) == true)
						{
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " lease found critical"<< " \n");
							
							
#endif	
							lease_critical = true;
							CREATE_SCOUT_THREAD;
							time(&lease_timestamp);
							//current_batch++;
							continue; //skip processing read commands till next update timeout
				
						}

					
					//issue read commands accumulated during this batch to this replica
					i=read_command_counter;
					if(read_commands[i] != -1)
					{
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " constructing decision for read commands"<< " \n");
							
							
#endif	
						//there is an accumulated read command
						//sending data in the format
						//DECISION:COMMANDER_ID:-1:COMMAND
						//printf("here\n");
						strcpy(send_buff,"DECISION");
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,-1);
						strcat(send_buff,DELIMITER);

					while(read_commands[i] != -1)
					{
					
						sprintf(send_buff,"%s%d",send_buff,read_commands[i]);
						strcat(send_buff,DELIMITER_SEC);
					
						i++;
					}
						strcat(send_buff,DELIMITER);

#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " sending read commands to replica " << recv_pid << " \n");
							printf("sending read commands at leader %d to replica %d\n",my_pid,recv_pid);
							
#endif
						ret = sendto(TALKER, send_buff, strlen(send_buff), 0, 
      								(struct sockaddr *)&replica_addr[recv_pid], replica_addr_len[recv_pid]);
			
						if (ret < 0)
     						{
      							perror("sendto ");
						        close(TALKER);
      							//return false;
     						}
					read_issued_till = i;
					//read timeout
					tread.tv_sec = read_timeout;
					tread.tv_usec = 0; 
					tptr = &tread;

					read_issued = true;
					}
					else
					{
#if DEBUG == 1
							LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " no read commands"<< " \n");
							printf("no read commands \n");
							
#endif	
						//no read commands to execute after this batch
						tupdate.tv_sec = update_timeout;
						tupdate.tv_usec = 0;
						tptr = &tupdate;
						if(noupdate == false)
						{
							current_batch++;
						}
						else
							noupdate = false; //do not increment current batch if it did not have any update commands
						i=(current_batch-1)*BATCH_SIZE;
						read_issued = false;
						while(leader_state.dlist.command[i] != -1)
						{

						//DECISION BELONGS TO CURRENT BATCH - BROADCAST TO REPLICAS

						//sending data in the format
						//DECISION:COMMANDER_ID:SLOT_NUMBER:COMMAND
						//printf("here\n");
						strcpy(send_buff,"DECISION");
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,i);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,leader_state.dlist.command[i]);
						strcat(send_buff,DELIMITER);

						if(broadcast_replicas(my_pid,TALKER,send_buff,replica_addr,replica_addr_len))
						{
#if DEBUG == 1
						LOG4CXX_TRACE(LeaderLogger,"LEader id: " << my_pid << " decision broadcasted \n");
						//printf("decision broadcasted at LEADER %d\n",my_pid);
							
#endif
	
						//remove entry from proposal list 	
						//leader_state.plist.command[slot_number] = -1;
						leader_state.dlist.command[slot_number] = -2;
						
						}
						else
						{
						LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " decision broadcast failed \n");			
						//printf("broadcast of decision failed at LEADER %d\n",my_pid);	
						}

						i++;
					
						}
						
					}
					}
				}
				

			}
			else if(strcmp(data,"READ-COMMIT") == 0)
			{
				read_command_counter = read_issued_till;
				read_issued = false;
				tupdate.tv_sec = update_timeout;
				tupdate.tv_usec = 0;
				tptr = &tupdate;
				if(noupdate == false)
				{
					current_batch++;
				}
				else
					noupdate = false; //do not increment current batch if it did not have any update commands
	
				i=(current_batch-1)*BATCH_SIZE;
				while(leader_state.dlist.command[i] != -1)
				{

					//DECISION BELONGS TO CURRENT BATCH - BROADCAST TO REPLICAS

						//sending data in the format
						//DECISION:COMMANDER_ID:SLOT_NUMBER:COMMAND
						//printf("here\n");
						strcpy(send_buff,"DECISION");
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,i);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,leader_state.dlist.command[i]);
						strcat(send_buff,DELIMITER);

					if(broadcast_replicas(my_pid,TALKER,send_buff,replica_addr,replica_addr_len))
					{
#if DEBUG == 1
						LOG4CXX_TRACE(LeaderLogger,"LEader id: " << my_pid << " decision broadcasted \n");
						//printf("decision broadcasted at LEADER %d\n",my_pid);
							
#endif
	
						//remove entry from proposal list 	
						//leader_state.plist.command[slot_number] = -1;
						leader_state.dlist.command[slot_number] = -2;
						
					}
					else
					{
						LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " decision broadcast failed \n");			
						//printf("broadcast of decision failed at LEADER %d\n",my_pid);	
					}

					i++;
					
				}
				latest_replica_pid = -1; //clear this to detect timeout on update commands

				
			}
			else
			{
				LOG4CXX_TRACE(LeaderLogger,"Leader id: " << my_pid << " undefined msg received"  << data << "\n");
				//printf("undefined msg received at leader id: %d msg:%s\n",my_pid,data);
			}
		}
		
		
	}	
return 0;
}
