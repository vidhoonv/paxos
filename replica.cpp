#include<stdio.h>
#include"pax_types.h"
#include<algorithm>

#define MAX_SET_SIZE 100
#define TALKER replica_comm.comm_fd[TALKER_INDEX]
#define LISTENER replica_comm.comm_fd[LISTENER_INDEX]

#define PROPOSE_COMMAND(command)  \
					for(i=0;i<MAX_SLOTS;i++) \
					{	\
						if(replica_state.decision_list.command[i] == -1 &&  \
								replica_state.proposal_list.command[i] == -1) \
						{ \
							/*slot is unused for decision or proposal*/ \
												\
							slot_number = i;		\
							break;				\
						} \
					} \
					if(i==MAX_SLOTS)	\
					{	\
						\
						printf("Ran out of slots!!!!!\n");	\
						return -1;	\
					}	\
						\
					if(client_cmdtype_map[command] == COMMAND_ACCBALANCE)	\
					{	\
						slot_number = -1;	\
					}	\
					replica_state.proposal_list.command[slot_number] = command; \
					/*minimum free slot has been found 
					//prepare proposal msg
					//sending data in the format
					//PROPOSE:REPLICA_ID:SLOTNUMBER:COMMAND:*/	\
					strcpy(send_buff,"PROPOSE");	\
					strcat(send_buff,DELIMITER);	\
					sprintf(send_buff,"%s%d",send_buff,my_pid);	\
					strcat(send_buff,DELIMITER);	\
					sprintf(send_buff,"%s%d",send_buff,slot_number);	\
					strcat(send_buff,DELIMITER);	\
					sprintf(send_buff,"%s%d",send_buff,command);	\
					strcat(send_buff,DELIMITER);	\
									\
					/*broadcast to all leaders*/	\
					if(broadcast_proposal(TALKER,send_buff,leader_addr,leader_addr_len))	\
					{	\
						printf("proposal broadcasted at replica %d -> (%d,%d)\n",my_pid,slot_number,command);	\
					}	\
					else	\
					{	\
						printf("broadcast of proposal failed at replica %d\n",my_pid);	\
						return -1;	\
					}	

/*
This will change along with client TENTATIVE
*/
#define PERFORM_COMMAND(command) \
			printf("\nIN PERFORM COMMAND %d\n",command); \
			for(i=0;i<replica_state.slot_number;i++)	\
			{	\
				if(replica_state.decision_list.command[i] == command)	\
				{	\
					/*this is a repeat command which was executed before*/ \
					printf("\nReceived REPEAT decision for command %d\n",command); \
					repeat = true; \
					/*skip command and update slot*/ \
					replica_state.slot_number += 1; \
					break;	\
				}	\
			}	\
			if(!repeat)	\
			{	\
				rc = do_command(my_pid,client_cmdtype_map[command],client_cmddata_map[command]);	\
				printf("\n>>>>>>>>Performed command %d res:%d\n",command,rc); \
				if(client_cmdtype_map[command] != READ_COMMAND)	\
				{	\
				replica_state.slot_number += 1; \
				replica_state.state += 1;	\
				}	\
				respond(my_pid,TALKER,command,client_addr[client_cmd_map[command]],client_addr_len[client_cmd_map[command]],rc);	\
			} \
			repeat = false; \

int ACCEPTOR_PORT_LIST[MAX_ACCEPTORS] = {3000,3002,3004};//,3006,3008,3010,3012,3014,3016,3018};
int LEADER_PORT_LIST[MAX_LEADERS] = {4000,4002,4003};
int REPLICA_PORT_LIST[MAX_REPLICAS] = {2000,2002};
int COMMANDER_PORT_LIST[MAX_COMMANDERS] = {5000,5001,5002,5003,5004,5005,5006,5007,5008,5009,5010,5011,5012,5013,5014,5015,5016,5017,5018,5019,5020,5021,5022,5023,5024,5025,5026,5027,5028,5029,5030,5031,5032,5033,5034,5035,5036,5037,5038,5039,5040,5041,5042,5043,5044,5045,5046,5047,5048,5049,5050,5051,5052,5053,5054,5055,5056,5057,5058,5059};
int SCOUT_PORT_LIST[MAX_SCOUTS] = {6000,6001,6002,6003,6004,6005,6006,6007,6008,6009,6010,6011,6012,6013,6014,6015,6016,6017,6018,6019,6020,6021,6022,6023,6024,6025,6026,6027,6028,6029,6030,6031,6032,6033,6034,6035,6036,6037,6038,6039,6040,6041,6042,6043,6044,6045,6046,6047,6048,6049,6060,6051,6052,6053,6054,6055,6056,6057,6058,6059};
//int CLIENT_PORT_LIST[MAX_CLIENTS] = {7000,7001};//,7002};
int CLIENT_PORT_LIST[MAX_CLIENTS] = {7000};//,7001};//,7002};

struct STATE_REPLICA {

	int slot_number;
	int state; //for time being this will be number of commands executed (same as slot number) but will serve as place holder
	struct PROPOSAL proposal_list; //<slot_number,command>
	struct PROPOSAL decision_list; //<slot_number,command>
};

int do_command(int my_pid,int cmd_type,char* cmd_data)
{
	FILE *fp,*fptemp;
	char filename[FILENAME_LENGTH];
	char tempname[FILENAME_LENGTH];
	int i=0,balance=0;
	size_t len;
	char *line=NULL,*data;
	ssize_t read;
	char acc_name[BUFSIZE/2],ac_nm[BUFSIZE/2];
	int  op_arg;
	bool fail=false;
	
	strcpy(filename,RESOURCE_FILE_PREFIX);
	sprintf(filename,"%s%d",filename,my_pid);
	strcat(filename,".res");

	strcpy(tempname,"temp");
	sprintf(tempname,"%s%d",tempname,my_pid);
	strcat(tempname,".res");	
	
	fp = fopen(filename,"a+");
	if(fp == NULL)
	{
		printf("file could not be accessed\n");
		return -1;
	}	
	printf("while doing command: cmd_type:%d cmd_data:%s\n",cmd_type,cmd_data);
	
	data = strtok(cmd_data,DELIMITER_CMD);
	if(!data)
		return -2;
	strcpy(acc_name,data);
	data = strtok(NULL,DELIMITER_CMD);
	if(!data)
		return -2;
	op_arg = atoi(data);

	//printf("while doing command: acc:%s arg:%d\n",acc_name,op_arg);
	switch(cmd_type)
	{
		case COMMAND_DEPOSIT:
					fail = true;
					//printf("!!!doing deposit\n");
					fptemp = fopen(tempname,"a+");
					if(fptemp == NULL)
					{
						printf("file could not be accessed\n");
						return -1;
					}	
					while(( read = getline(&line,&len,fp)) != -1)
					{
						if(line[strlen(line)-1] == '\n')
						{				
							line[strlen(line)-1]='\0';
						}
						sscanf(line,"%s %d",ac_nm,&balance);
						//printf("from file: acc name %sac_nm:%s bal %d\n",acc_name,ac_nm,balance);
						if(strcmp(ac_nm,acc_name) == 0)
						{
							fprintf(fptemp,"%s %d\n",acc_name,balance+op_arg);  //adding deposit amount to store new balance
							fail =  false;
							continue;				
						}
						else
						{
						//printf("not equal %d\n",strcmp(ac_nm,acc_name));
						}
						fprintf(fptemp,"%s\n",line);
			
					}
					fclose(fptemp);
					fclose(fp);
					if(fail == true)
					{
						if(unlink(tempname) == -1)  //deleting old log file
							printf("deleting temp resource file failed\n");
						else
							printf("temp res file deleted\n");
						return -1;
	
					}
				//	printf("!!!deposit complete\n");
					if(unlink(filename) == -1)  //deleting old log file
						printf("deleting old resource file failed\n");
					else
						printf("old res file deleted\n");
					if(rename(tempname,filename) == -1) //rename 
						printf("new resource file rename failed\n");
					else
						printf("new resource file ready\n");
					break;
		case COMMAND_WITHDRAW:
					fail=true;
					fptemp = fopen(tempname,"a+");
					if(fptemp == NULL)
					{
						printf("file could not be accessed\n");
						return -1;
					}	
					while(( read = getline(&line,&len,fp)) != -1)
					{
						if(line[strlen(line)-1] == '\n')
						{				
							line[strlen(line)-1]='\0';
						}
						sscanf(line,"%s %d",ac_nm,&balance);
						//printf("from file: acc name %sac_nm:%s bal %d\n",acc_name,ac_nm,balance);
						if(strcmp(ac_nm,acc_name) == 0)
						{

							if(balance>=op_arg)
							{
								fprintf(fptemp,"%s %d\n",acc_name,balance-op_arg);  //adding deposit amount to store new balance
								fail=false;
								continue;
							}
							else
							{
								fail = true;
								break;
							}				
						}
						fprintf(fptemp,"%s\n",line);
			
					}
					fclose(fptemp);
					fclose(fp);
					if(fail == true)
					{
						if(unlink(tempname) == -1)  //deleting old log file
							printf("deleting temp resource file failed\n");
						else
							printf("temp res file deleted\n");
						return -1;
	
					}
					if(unlink(filename) == -1)  //deleting old log file
						printf("deleting old resource file failed\n");
					else
						printf("old res file deleted\n");
					if(rename(tempname,filename) == -1) //rename 
						printf("new resource file rename failed\n");
					else
						printf("new resource file ready\n");
					break;
				
					break;
		case COMMAND_ACCBALANCE:
					fptemp = fopen(tempname,"a+");
					if(fptemp == NULL)
					{
						printf("file could not be accessed\n");
						return -1;
					}	
					while(( read = getline(&line,&len,fp)) != -1)
					{
						if(line[strlen(line)-1] == '\n')
						{				
							line[strlen(line)-1]='\0';
						}
						sscanf(line,"%s %d",ac_nm,&balance);
						if(strcmp(ac_nm,acc_name) == 0)
						{
							//required entry found - return balance
							printf("Executing read command for %s - balance:%d\n",ac_nm,balance);
							return balance;
						}
					}
						return -1; //entry not found 
					break;

		default:
				printf("!!!some invalid command\n");
				return -1;

	}	
return 0;

}
void respond(int my_pid,int talker_fd,int command_id,struct sockaddr dest_addr, socklen_t dest_addr_len,int result)
{

	int ret;
	char send_buff[BUFSIZE];

	printf("\nSending response to client\n");
	strcpy(send_buff,"RESPONSE"); 	
	strcat(send_buff,DELIMITER);
	sprintf(send_buff,"%s%d",send_buff,my_pid);	
	strcat(send_buff,DELIMITER);
	sprintf(send_buff,"%s%d",send_buff,command_id);	
	strcat(send_buff,DELIMITER);

	if(result == -1)
	{
		
		strcat(send_buff,"F");

	}
	else if(result == 0)
	{
		
		strcat(send_buff,"S");
	}
	else
	{
		sprintf(send_buff,"%s%d",send_buff,result);
	}
	strcat(send_buff,DELIMITER);
	

	ret = sendto(talker_fd, send_buff, strlen(send_buff), 0, 
      		(struct sockaddr *)&dest_addr, dest_addr_len);
			
	if (ret < 0)
     	{
      		perror("sendto ");
	        close(talker_fd);
      		//return false;
     	}
//return true;	
}

bool configure_replica(int my_pid,struct COMM_DATA *comm_replica)
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
	listener_addr.sin_port = htons(REPLICA_PORT_LIST[my_pid]); 

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
    	printf("listener using port %d\n", ntohs(listener_addr.sin_port));

	//talker setup
	if (( talker_fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0){
			perror("talker socket ");
       			return false;
	}
	printf("talker_fd = %d\n",talker_fd);

	talker_addr.sin_family = AF_INET;
	talker_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    	talker_addr.sin_port = htons(0);  // pick any free port

	if (bind(talker_fd, (struct sockaddr *) &talker_addr, sizeof(talker_addr)) < 0)
    	{
        perror("talker bind ");
        close(talker_fd);
        return false;
    	}   

	comm_replica->comm_fd[LISTENER_INDEX] = listener_fd;
	comm_replica->comm_fd[TALKER_INDEX] = talker_fd;

return true;
}
bool broadcast_proposal(int talker_fd,char send_buff[],struct sockaddr dest_addr[],socklen_t dest_addr_len[])
{
	int i,ret;
	for(i=0;i<MAX_LEADERS;i++)
	{
		ret = sendto(talker_fd, send_buff, strlen(send_buff), 0, 
      			(struct sockaddr *)&dest_addr[i], dest_addr_len[i]);
			
		if (ret < 0)
     		{
      			perror("sendto ");
		        close(talker_fd);
      			return false;
     		}
		
	}
return true;	
}
int main(int argc, char **argv)
{
	struct COMM_DATA replica_comm;
	struct STATE_REPLICA replica_state;
	int my_pid;
	int rc=0;

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
	struct sockaddr_in *leader_addr_in[MAX_LEADERS],*client_addr_in[MAX_CLIENTS];
	struct sockaddr leader_addr[MAX_LEADERS],client_addr[MAX_CLIENTS];
	socklen_t leader_addr_len[MAX_LEADERS],client_addr_len[MAX_CLIENTS];
//misc
	int i,ret=0,recv_pid;
	char buff_copy[BUFSIZE];
	int slot_number,command;
	char *data,*cstr,*cmd_str,*tok;
	bool repeat= false;
	bool first_request = false;
	int recent_leader_pid = -1;

//tentative
	int client_cmd_map[MAX_SLOTS];
	enum COMMAND_TYPE client_cmdtype_map[MAX_SLOTS];
	char client_cmddata_map[MAX_SLOTS][BUFSIZE/2];

//batch details
	int current_batch = 1;

//initialization
	std::fill(replica_state.proposal_list.command, replica_state.proposal_list.command+MAX_SLOTS, -1);
	std::fill(replica_state.decision_list.command, replica_state.decision_list.command+MAX_SLOTS, -1);
	std::fill(client_cmd_map, client_cmd_map+MAX_SLOTS, -1);

	replica_state.slot_number =0;
	replica_state.state =0;
//check runtime arguments
	if(argc!=2)
	{
		printf("Usage: ./replica <replica_id>\n");
		return -1;
	}
	my_pid=atoi(argv[1]);

	//hostname configuration
	gethostname(hostname, sizeof(hostname));
	hp = gethostbyname(hostname);
	if (hp == NULL) 
	{ 
		printf("\n%s: unknown host.\n", hostname); 
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

//setup client addresses

	for(i=0;i<MAX_CLIENTS;i++)
	{
		client_addr_in[i] = (struct sockaddr_in *)&(client_addr[i]);
		client_addr_in[i]->sin_family = AF_INET;
		memcpy(&client_addr_in[i]->sin_addr, hp->h_addr, hp->h_length); 
		client_addr_in[i]->sin_port  = htons(CLIENT_PORT_LIST[i]);  
		client_addr_len[i] = sizeof(client_addr[i]);
	}

	//configure replica talker and listener ports	
	//setup the replica
	if(configure_replica(my_pid,&replica_comm))
	{
		printf("Replica id: %d configured successfully\n",my_pid);
	}
	else
	{
		printf("Error in config of replica id: %d\n",my_pid);
		return -1;
	}

	while(1)
	{
		maxfd = LISTENER+1;
		FD_ZERO(&readfds); 
		FD_SET(LISTENER, &readfds);

		ret = select(maxfd, &readfds, NULL, NULL, NULL);  //blocks forever till it receives a message


		if(ret <0)
	   	{ 
	     		printf("\nSelect error\n");   
	     		return -1;
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
  			printf("received: %s\n", recv_buff);

			strcpy(buff_copy,recv_buff);			
			data = strtok(buff_copy,DELIMITER);

//retrive recv_pid
				recv_pid = atoi(strtok(NULL,DELIMITER));
#if DEBUG==1
				printf("recved msg from %d\n",recv_pid);
#endif		
			if(strcmp(data,"REQUEST") == 0)
			{
				//recved from client
				//expects data in the format
				//REQUEST:<CLIENT_ID>:<COMMAND_STR>:

				//retrive command string
				cstr = strtok(NULL,DELIMITER);

				command = atoi(strtok(cstr,DELIMITER_SEC));
printf("\n\nCurrent slot number %d\n\n",replica_state.slot_number);
				if(client_cmd_map[command] == -1)
				{
					client_cmdtype_map[command]= (enum COMMAND_TYPE)atoi(strtok(NULL,DELIMITER_SEC));
					strcpy(client_cmddata_map[command],strtok(NULL,DELIMITER_SEC));				
					client_cmd_map[command] = recv_pid;
					first_request = true;
				}
				if(replica_state.decision_list.command[replica_state.slot_number] == command)
				{
					//getting data for a command whose decision has already arrived
					PERFORM_COMMAND(command);

					if(replica_state.slot_number == current_batch*BATCH_SIZE)
					{
						//EXECUTION OF CURRENT BATCH COMPLETE.. SEND COMMIT
						
						printf("\nSending commit to leader\n");
						strcpy(send_buff,"COMMIT"); 	
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);	
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,replica_state.slot_number);	
						strcat(send_buff,DELIMITER);

						ret = sendto(TALKER, send_buff, strlen(send_buff), 0, 
      							(struct sockaddr *)&leader_addr[recent_leader_pid], leader_addr_len[recent_leader_pid]);
			
						if (ret < 0)
     						{
     					 		perror("sendto ");
						        close(TALKER);
      							//return false;
     						}

						current_batch++;
					}
	
				} 
				else
				{				
					//check for repeat request
					for(i=0;i<MAX_SLOTS;i++)
					{

						if(command == replica_state.decision_list.command[i] && first_request == false)
						{
							//decision has already made
							repeat = true;

						}

					}
					if(!repeat)
					{
					
						PROPOSE_COMMAND(command);
					}	
					repeat = false; //clear flag
				}
			}
			else if(strcmp(data,"DECISION") == 0)
			{
				//recved from commander 
				//expects data in the format
				//DECISION:COMMANDER_ID:SLOT_NUMBER:COMMAND:

				//retrive decision components

				slot_number = atoi(strtok(NULL,DELIMITER));
				

				if(slot_number == -1)
				{
					//this is a read command

					cstr = strtok(NULL,DELIMITER);
					cmd_str = strtok_r(cstr,DELIMITER_SEC,&tok);
					while(cmd_str)
					{
						command =  atoi(cmd_str);
						PERFORM_COMMAND(command);
						cmd_str = strtok_r(NULL,DELIMITER_SEC,&tok);
					}

					//send READ-COMMIT to leader
						//EXECUTION OF READ COMMANDS COMPLETE.. SEND READ COMMIT
						
						printf("\nSending read commit to leader\n");
						strcpy(send_buff,"READ-COMMIT"); 	
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);	
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,command);	
						strcat(send_buff,DELIMITER);

						ret = sendto(TALKER, send_buff, strlen(send_buff), 0, 
      							(struct sockaddr *)&leader_addr[recv_pid], leader_addr_len[recv_pid]);
			
						if (ret < 0)
     						{
     					 		perror("sendto ");
						        close(TALKER);
      							//return false;
     						}

					continue;
				}
				command = atoi(strtok(NULL,DELIMITER));
				//add to decision list
				if(slot_number < replica_state.slot_number && slot_number%BATCH_SIZE == BATCH_SIZE-1)
				{

					//received a repeat decision for an old slot that is already updated 

					//just send commit (happens during leader failure)

						//EXECUTION OF CURRENT BATCH COMPLETE.. SEND COMMIT
						
						printf("\nSending commit to leader\n");
						strcpy(send_buff,"COMMIT"); 	
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);	
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,slot_number+1);	
						strcat(send_buff,DELIMITER);

						ret = sendto(TALKER, send_buff, strlen(send_buff), 0, 
      							(struct sockaddr *)&leader_addr[recv_pid], leader_addr_len[recv_pid]);
			
						if (ret < 0)
     						{
     					 		perror("sendto ");
						        close(TALKER);
      							//return false;
     						}


					continue;
				}

				replica_state.decision_list.command[slot_number] = command;


				//execute decisions from current slot number
				while(replica_state.decision_list.command[replica_state.slot_number] != -1 && client_cmd_map[replica_state.decision_list.command[replica_state.slot_number]] != -1) //check for slot number and also for arrival of command data (replica cant perform command before arrival of command data
				{
					//decision is available for current slot_number

					//check proposal_list and revise if required
					if(replica_state.proposal_list.command[replica_state.slot_number] != -1 && 							replica_state.proposal_list.command[replica_state.slot_number] != 									replica_state.decision_list.command[replica_state.slot_number])
					{		
						//the slot was used for some proposal
						//revise and repropose for some other slot
						
						command = replica_state.proposal_list.command[replica_state.slot_number];
						printf("proposing again for command %d found at slot %d!!!!\n",command,slot_number);
						PROPOSE_COMMAND(command);
					}
					//printf("!!!trying to perform command %d\n",command);
					command = replica_state.decision_list.command[replica_state.slot_number];
					PERFORM_COMMAND(command);

					if(replica_state.slot_number == current_batch*BATCH_SIZE)
					{
						//EXECUTION OF CURRENT BATCH COMPLETE.. SEND COMMIT
						
						printf("\nSending commit to leader\n");
						strcpy(send_buff,"COMMIT"); 	
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);	
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,replica_state.slot_number);	
						strcat(send_buff,DELIMITER);

						ret = sendto(TALKER, send_buff, strlen(send_buff), 0, 
      							(struct sockaddr *)&leader_addr[recv_pid], leader_addr_len[recv_pid]);
			
						if (ret < 0)
     						{
     					 		perror("sendto ");
						        close(TALKER);
      							//return false;
     						}

						current_batch++;
					}	
				}

				recent_leader_pid = recv_pid;

			}
			else
			{
				printf("undefined msg received at replica id: %d msg:%s\n",my_pid,data);
			}

		}

	}
return 0;
}
