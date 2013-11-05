#include<stdio.h>
#include "pax_types.h"

#define LISTENER_TEST_PORT 2312

#define TALKER acceptor_comm.comm_fd[TALKER_INDEX]
#define LISTENER acceptor_comm.comm_fd[LISTENER_INDEX]

#define MAX_SET_SIZE 100


#define ACCEPTED_STRING_PREP(STR,ACCEPTED_SET)  strcpy(STR,""); \
						for(i=0;i<ACCEPTED_SET.current_length;i++) \
						{ \
							BALLOT_STRING_PREP(cstr,ACCEPTED_SET.ballot[i]); \
							strcat(STR,cstr); \
							sprintf(STR,"%s%d",STR,ACCEPTED_SET.slot_number[i]); \
							strcat(STR,DELIMITER_SEC);  \
							sprintf(STR,"%s%d",STR,ACCEPTED_SET.command[i]); \
							strcat(STR,DELIMITER_SEC); \
						} 
 	
int ACCEPTOR_PORT_LIST[MAX_ACCEPTORS] = {3000,3002,3004};//,3006,3008,3010,3012,3014,3016,3018};
int LEADER_PORT_LIST[MAX_LEADERS] = {4000,4002};
int REPLICA_PORT_LIST[MAX_REPLICAS] = {2000,2002};
int COMMANDER_PORT_LIST[MAX_COMMANDERS] = {5000,5001,5002,5003,5004,5005,5006,5007,5008,5009,5010,5011,5012,5013,5014,5015,5016,5017,5018,5019,5020,5021,5022,5023,5024,5025,5026,5027,5028,5029,5030,5031,5032,5033,5034,5035,5036,5037,5038,5039,5040,5041,5042,5043,5044,5045,5046,5047,5048,5049,5050,5051,5052,5053,5054,5055,5056,5057,5058,5059};
int SCOUT_PORT_LIST[MAX_SCOUTS] = {6000,6001,6002,6003,6004,6005,6006,6007,6008,6009,6010,6011,6012,6013,6014,6015,6016,6017,6018,6019,6020,6021,6022,6023,6024,6025,6026,6027,6028,6029,6030,6031,6032,6033,6034,6035,6036,6037,6038,6039,6040,6041,6042,6043,6044,6045,6046,6047,6048,6049,6060,6051,6052,6053,6054,6055,6056,6057,6058,6059};
struct ACCEPTED_SET
{
	struct BALLOT_NUMBER ballot[MAX_SET_SIZE];
	int slot_number[MAX_SET_SIZE];
	int command[MAX_SET_SIZE];	//temporarily considered as int
	int current_length;
};

struct STATE_ACCEPTOR
{
	struct BALLOT_NUMBER ballot;
	struct ACCEPTED_SET  accepted;
	
};

bool respond(int talker_fd, struct sockaddr leader_addr, char send_buff[])
{

	int ret;
	socklen_t leader_addr_len;


	leader_addr_len = sizeof(leader_addr);

	ret = sendto(talker_fd, send_buff, strlen(send_buff), 0, 
      		(struct sockaddr *)&leader_addr, leader_addr_len);
			
	if (ret < 0)
     	{
      		perror("sendto ");
	        close(talker_fd);
      		return false;
     	}
return true;	
}

int ballot_compare(struct BALLOT_NUMBER other_ballot,struct BALLOT_NUMBER my_ballot)
{
	if(other_ballot.bnum-my_ballot.bnum == 0)
	{
#if DEBUG==1
		printf("ballot compare returning %d\n",other_ballot.leader_id-my_ballot.leader_id);
#endif	
		return (other_ballot.leader_id-my_ballot.leader_id);	
	}
	else
	{
#if DEBUG==1
		printf("ballot compare returning %d\n",other_ballot.bnum-my_ballot.bnum);
#endif	
		return(other_ballot.bnum-my_ballot.bnum); 
	}
}
void ballot_copy(struct BALLOT_NUMBER other_ballot,struct BALLOT_NUMBER *my_ballot)
{
	my_ballot->bnum = other_ballot.bnum;
	my_ballot->leader_id = other_ballot.leader_id;
}
bool configure_acceptor(int my_pid,struct COMM_DATA *comm_acc)
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
	listener_addr.sin_port = htons(ACCEPTOR_PORT_LIST[my_pid]); 

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

	comm_acc->comm_fd[LISTENER_INDEX] = listener_fd;
	comm_acc->comm_fd[TALKER_INDEX] = talker_fd;

return true;
}
int main(int argc,char **argv)
{

	int my_pid;
	struct COMM_DATA acceptor_comm;
	struct STATE_ACCEPTOR acc_state;

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
	int ret = 0;
	struct sockaddr_in *commander_addr_in[MAX_COMMANDERS],*scout_addr_in[MAX_SCOUTS];
	struct sockaddr commander_addr[MAX_COMMANDERS],scout_addr[MAX_SCOUTS];
	socklen_t commander_addr_len[MAX_COMMANDERS],scout_addr_len[MAX_SCOUTS];

//comm common
	struct sockaddr_storage temp_paddr;
	socklen_t temp_paddr_len;

//testing comm
	struct sockaddr_in *process_addr_in;
	struct sockaddr process_addr;
	socklen_t process_addr_len;
	
//string processing
	char *data;
	char *ballot_str;
	char buff_copy[BUFSIZE];
	char bstr[BUFSIZE],cstr[BUFSIZE];
	char accepted_str[BUFSIZE];

//sender details of a received message
	int recv_pid;

//misc
	int i=0;

//protocol related
	int slot_number=0, command=0;
	struct BALLOT_NUMBER recv_ballot;
//check runtime arguments
	if(argc!=2)
	{
		printf("Usage: ./acc <acceptor_id>\n");
		return -1;
	}
	my_pid=atoi(argv[1]);

//initializations
	acc_state.accepted.current_length = 0;
//hostname configuration
	gethostname(hostname, sizeof(hostname));
	hp = gethostbyname(hostname);
	if (hp == NULL) 
	{ 
		printf("\n%s: unknown host.\n", hostname); 
		return 0; 
	} 

//setup the acceptor 
	if(configure_acceptor(my_pid,&acceptor_comm))
	{
		printf("Acceptor id: %d configured successfully\n",my_pid);
	}
	else
	{
		printf("Error in config of acceptor id: %d\n",my_pid);
		return -1;
	}

//setup commander addresses

	for(i=0;i<MAX_COMMANDERS;i++)
	{
		commander_addr_in[i] = (struct sockaddr_in *)&(commander_addr[i]);
		commander_addr_in[i]->sin_family = AF_INET;
		memcpy(&commander_addr_in[i]->sin_addr, hp->h_addr, hp->h_length); 
		commander_addr_in[i]->sin_port  = htons(COMMANDER_PORT_LIST[i]);  
		commander_addr_len[i] = sizeof(commander_addr[i]);
	}
//setup scout addresses
	for(i=0;i<MAX_SCOUTS;i++)
	{
		scout_addr_in[i] = (struct sockaddr_in *)&(scout_addr[i]);
		scout_addr_in[i]->sin_family = AF_INET;
		memcpy(&scout_addr_in[i]->sin_addr, hp->h_addr, hp->h_length); 
		scout_addr_in[i]->sin_port  = htons(SCOUT_PORT_LIST[i]);
		scout_addr_len[i] = sizeof(scout_addr[i]);  
	}
/*
//test listener and talker

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

	process_addr_in = (struct sockaddr_in *)&(process_addr);
	process_addr_in->sin_family = AF_INET;
	memcpy(&process_addr_in->sin_addr, hp->h_addr, hp->h_length); 
	process_addr_in->sin_port  = htons(2312);  
	
	strcpy(send_buff,"hello tester!");					
	
	process_addr_len = sizeof(process_addr);
	ret = sendto(TALKER, send_buff, strlen(send_buff), 0, 
      		(struct sockaddr *)&process_addr, process_addr_len);
			
	if (ret < 0)
     	{
      		perror("sendto ");
	        close(TALKER);
      		return 0;
     	}
*/
//wait for messages from leader

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
/*			
			if(strcmp(data,"hello_world") == 0) //testing
			{
				//retrive recv_pid
				recv_pid = atoi(strtok(NULL,DELIMITER));
				printf("recv from %d\n",recv_pid);
				//test reply
				if(respond(TALKER,leader_addr[recv_pid],"thank you for the kind welcome!"))
				{
					printf("responded to leader %d",recv_pid);
				}
			}
*/
			if(strcmp(data,"PHASE1_REQUEST") == 0)
			{

				//recved from SCOUT
			
				//expects data in the format
				//PHASE1_REQUEST:<SCOUT_ID>:<BALLOT>:
	
				//retrive components of the recv_ballot
				ballot_str = strtok(NULL,DELIMITER);
				recv_ballot.bnum = atoi(strtok(ballot_str,DELIMITER_SEC));
				recv_ballot.leader_id = atoi(strtok(NULL,DELIMITER_SEC));
#if DEBUG==1
				printf("recv ballot round: %d lid: %d\n",recv_ballot.bnum,recv_ballot.leader_id);
#endif	
				if(ballot_compare(recv_ballot,acc_state.ballot) > 0) 
				{
					//received ballot should be adopted
					//ballot_copy(recv_ballot,&acc_state.ballot); //to be defined
					acc_state.ballot.bnum = recv_ballot.bnum;
					acc_state.ballot.leader_id = recv_ballot.leader_id;
#if DEBUG==1
					printf("ballot round: %d lid: %d accepted\n",acc_state.ballot.bnum,acc_state.ballot.leader_id);
#endif	
				}
				else
				{
#if DEBUG==1
				printf("recv ballot round: %d lid: %d not accepted\n",recv_ballot.bnum,recv_ballot.leader_id);
#endif	
				}
				//else just skip the ballot

				//send PHASE1_RESPONSE 
	
				//sending data in the format
				//PHASE1_RESPONSE:<ACCEPTOR_ID>:<BALLOT>:<ACCEPTED_SET>:

				//prepare ballot string
				BALLOT_STRING_PREP(bstr,acc_state.ballot);
#if DEBUG==1
				printf("phase 1 response bstr: %s ballot(%d,%d)\n",bstr,acc_state.ballot.bnum,acc_state.ballot.leader_id);
#endif	
				
				strcpy(send_buff,"PHASE1_RESPONSE");
				strcat(send_buff,DELIMITER);
				sprintf(send_buff,"%s%d",send_buff,my_pid); //acceptor id
				strcat(send_buff,DELIMITER);
				strcat(send_buff,bstr);  //ballot
				strcat(send_buff,DELIMITER);
//prepare accepted string
				ACCEPTED_STRING_PREP(accepted_str,acc_state.accepted);
				strcat(send_buff,accepted_str); //accepted set
				strcat(send_buff,DELIMITER);
#if DEBUG==1
				printf("phase 1 response content: %s \n",send_buff);
#endif					
				respond(TALKER,scout_addr[recv_pid],send_buff);
				
			}
			else if(strcmp(data,"PHASE2_REQUEST") == 0)
			{
				//recved from COMMANDER

				//expects data in the format
				//PHASE2_REQUEST:<COMMANDER_ID>:<BALLOT>:<SLOT_NUMBER>:<COMMAND>:

				ballot_str = strtok(NULL,DELIMITER);
				
				//retrive slot num
				slot_number = atoi(strtok(NULL,DELIMITER));
			
				//retrive command
				command = atoi(strtok(NULL,DELIMITER));

				//retrive components of the recv_ballot
				recv_ballot.bnum = atoi(strtok(ballot_str,DELIMITER_SEC));
				recv_ballot.leader_id = atoi(strtok(NULL,DELIMITER_SEC));

				

				if(ballot_compare(recv_ballot,acc_state.ballot) >= 0)
				{
					//received ballot should be accepted
					ballot_copy(recv_ballot,&acc_state.ballot); 
					//include the pvalue in accepted set
					acc_state.accepted.ballot[acc_state.accepted.current_length].bnum = recv_ballot.bnum;
					acc_state.accepted.ballot[acc_state.accepted.current_length].leader_id = recv_ballot.leader_id;
					acc_state.accepted.slot_number[acc_state.accepted.current_length] = slot_number;
					acc_state.accepted.command[acc_state.accepted.current_length] = command;

					acc_state.accepted.current_length++; 
#if DEBUG==1
				printf("new ballot from commander %d accepted <%d:%d>\n",recv_pid, acc_state.ballot.bnum, 								acc_state.ballot.leader_id);
#endif									
				}
				
				//send PHASE2_RESPONSE
				//sending data in the format
				//PHASE2_RESPONSE:<ACCEPTOR_ID>:<BALLOT>:
				
				//prepare ballot string
				BALLOT_STRING_PREP(bstr,acc_state.ballot);
				strcpy(send_buff,"PHASE2_RESPONSE");
				strcat(send_buff,DELIMITER);
				sprintf(send_buff,"%s%d",send_buff,my_pid);
				strcat(send_buff,DELIMITER);
				strcat(send_buff,bstr);
				strcat(send_buff,DELIMITER);

#if DEBUG==1
				printf("phase 2 response content: %s \n",send_buff);
#endif	
				respond(TALKER,commander_addr[recv_pid],send_buff);
				
			}
			else 
			{
				printf("undefined msg received at acceptor id: %d msg:%s\n",my_pid,data);
			}

		}		
	}



return 0;
}
