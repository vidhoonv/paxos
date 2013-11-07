//logging
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

#include<stdio.h>
#include<pthread.h>
#include "pax_types.h"

using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

// Define static logger variable
LoggerPtr CommLogger(Logger::getLogger("commander"));

#define TALKER_COMMANDER commander_comm.comm_fd[TALKER_INDEX]
#define LISTENER_COMMANDER commander_comm.comm_fd[LISTENER_INDEX]

#define PREPARE_PROPOSAL_STR(STR,PROP) BALLOT_STRING_PREP(bstr,PROP.ballot); \
					strcpy(STR,bstr); \
					strcat(STR,DELIMITER); \
					sprintf(STR,"%s%d",STR,PROP.slot_number); \
					strcat(STR,DELIMITER); \
					sprintf(STR,"%s%d",STR,PROP.command); \
					strcat(STR,DELIMITER); 

#define PREPARE_PREEMPTED_MSG(STR,BALLOT) BALLOT_STRING_PREP(bstr,BALLOT); \
					strcpy(STR,"PREEMPTED"); \
					strcat(STR,DELIMITER); \
					sprintf(STR,"%s%d",STR,my_pid); \
					strcat(STR,DELIMITER); \
					strcat(STR,bstr); \
					strcat(STR,DELIMITER); 


extern int ACCEPTOR_PORT_LIST[MAX_ACCEPTORS], LEADER_PORT_LIST[MAX_LEADERS] ;
extern int REPLICA_PORT_LIST[MAX_REPLICAS];
extern int SCOUT_PORT_LIST[MAX_SCOUTS],COMMANDER_PORT_LIST[MAX_COMMANDERS];
int ballot_compare(struct BALLOT_NUMBER,struct BALLOT_NUMBER);

/*
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
*/
			
bool configure_commander(int my_pid,struct COMM_DATA *comm_commander)
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
	listener_addr.sin_port = htons(COMMANDER_PORT_STARTER+my_pid); 

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

	comm_commander->comm_fd[LISTENER_INDEX] = listener_fd;
	comm_commander->comm_fd[TALKER_INDEX] = talker_fd;

return true;
}

bool broadcast_proposal(int talker_fd,int my_pid,struct PVAL proposal,struct sockaddr dest_addr[],socklen_t dest_addr_len[])
{
	char pstr[BUFSIZE],cmd_str[BUFSIZE];
	char send_buff[BUFSIZE];
	int i,ret;
	char bstr[BUFSIZE];

	PREPARE_PROPOSAL_STR(pstr,proposal);
	//data sent in the format
	//PHASE2_REQUEST:<COMMANDER_ID>:<PVAL>:
	strcpy(send_buff,"PHASE2_REQUEST");
	strcat(send_buff,DELIMITER);
	sprintf(send_buff,"%s%d",send_buff,my_pid);
	strcat(send_buff,DELIMITER);
	strcat(send_buff,pstr);
	strcat(send_buff,DELIMITER);

	for(i=0;i<MAX_ACCEPTORS;i++)
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
bool broadcast_replicas(int talker_fd,char send_buff[],struct sockaddr replica_addr[],socklen_t replica_addr_len[])
{
	int i,ret;
	for(i=0;i<MAX_REPLICAS;i++)
	{
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
bool send_preemption(int talker_fd,int parent_id,char send_buff[],struct sockaddr parent_addr, socklen_t parent_addr_len)
{

	int ret;
	
	ret = sendto(talker_fd, send_buff, strlen(send_buff), 0, 
      			(struct sockaddr *)&parent_addr, parent_addr_len);
			
		if (ret < 0)
     		{
      			perror("sendto ");
		        close(talker_fd);
      			return false;
     		}
return true;
}
bool send_success(int talker_fd,int parent_id,char send_buff[],struct sockaddr parent_addr, socklen_t parent_addr_len)
{

	int ret;
	
	ret = sendto(talker_fd, send_buff, strlen(send_buff), 0, 
      			(struct sockaddr *)&parent_addr, parent_addr_len);
			
		if (ret < 0)
     		{
      			perror("sendto ");
		        close(talker_fd);
      			return false;
     		}
return true;
}
void* commander(void *thread_data) //acceptor list and replica list is global
{
	int not_accepted = MAX_ACCEPTORS;
	int i;
	struct COMM_DATA commander_comm;
//thread data
	int parent_id,my_pid;
	struct PVAL my_pval;
//comm related variables
	struct hostent *hp;
	char hostname[64];

//comm listening variables
	fd_set readfds;
	int maxfd;
	char rec_buff[BUFSIZE];
	int nread=0;	

//comm sending variables
	char send_buff[BUFSIZE];
	int ret = 0;
	struct sockaddr_in *replica_addr_in[MAX_REPLICAS],*acceptor_addr_in[MAX_ACCEPTORS];
	struct sockaddr replica_addr[MAX_REPLICAS],acceptor_addr[MAX_ACCEPTORS];
	socklen_t replica_addr_len[MAX_REPLICAS],acceptor_addr_len[MAX_ACCEPTORS];
	//parent address
	struct sockaddr_in *parent_addr_in;
	struct sockaddr parent_addr;
	socklen_t parent_addr_len;
//comm common
	struct sockaddr_storage temp_paddr;
	socklen_t temp_paddr_len;

//sender details of a received message
	int recv_pid;
	struct BALLOT_NUMBER recv_ballot;

//misc
	char *balot_str,*data,*tok,*tok1;
	char bstr[BUFSIZE];
	char buf_copy[BUFSIZE],cmd_str[BUFSIZE];
//thread data struct
	struct COMMANDER_THREAD_ARG *args;

//configure thread data
	args = (struct COMMANDER_THREAD_ARG*)thread_data;
	parent_id = args->parent_id;
	my_pid = args->my_pid;
	my_pval = args->my_pval;
//configure logger
 DOMConfigurator::configure("commander_log_config.xml");
//hostname configuration
	gethostname(hostname, sizeof(hostname));
	hp = gethostbyname(hostname);
	if (hp == NULL) 
	{ 
		LOG4CXX_TRACE(CommLogger,hostname << " : unknown host\n");
		return NULL; 
	} 


	//configure sender and listener
	if(configure_commander(my_pid,&commander_comm))
	{
		LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " configured successfully\n");
	}
	else
	{
		LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " config error \n");
		return NULL;
	}
	
//setup acceptor addresses

	for(i=0;i<MAX_ACCEPTORS;i++)
	{
		acceptor_addr_in[i] = (struct sockaddr_in *)&(acceptor_addr[i]);
		acceptor_addr_in[i]->sin_family = AF_INET;
		memcpy(&acceptor_addr_in[i]->sin_addr, hp->h_addr, hp->h_length); 
		acceptor_addr_in[i]->sin_port  = htons(ACCEPTOR_PORT_LIST[i]);  
		acceptor_addr_len[i] = sizeof(acceptor_addr[i]);
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

//setup parent address	(will be used for preemption
	parent_addr_in = (struct sockaddr_in *)&(parent_addr);
	parent_addr_in->sin_family = AF_INET;
	memcpy(&parent_addr_in->sin_addr, hp->h_addr, hp->h_length); 
	parent_addr_in->sin_port  = htons(LEADER_PORT_LIST[parent_id]);  
	parent_addr_len = sizeof(parent_addr);
//send proposal to all acceptors
	if(broadcast_proposal(TALKER_COMMANDER,my_pid,my_pval,acceptor_addr,acceptor_addr_len))
	{
#if DEBUG == 1
		LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " pval broadcasted \n");
		//printf("pval broadcasted at commander %d\n",my_pid);
#endif
	}
	else
	{
		LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " pval broadcast failed \n");
		//printf("broadcast of received pval failed at commander %d\n",my_pid);	
		return NULL;	
	}

//wait for reply

	while(1)
	{
		maxfd = LISTENER_COMMANDER+1;
		FD_ZERO(&readfds); 
		FD_SET(LISTENER_COMMANDER, &readfds);

		ret = select(maxfd, &readfds, NULL, NULL, NULL);  //blocks forever till it receives a message


		if(ret <0)
	   	{ 
			LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " select error \n");
	     		//printf("\nSelect error\n");   
	     		return NULL;
	   	} 

		if(FD_ISSET (LISTENER_COMMANDER, &readfds))
		{
			temp_paddr_len = sizeof(temp_paddr);
			nread = recvfrom (LISTENER_COMMANDER, rec_buff, BUFSIZE, 0, 
               	       			(struct sockaddr *)&temp_paddr, &temp_paddr_len); 
		
		 	if (nread < 0)
		       	{
				LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " recv from failed \n");
		        	perror("recvfrom ");
            			close(LISTENER_COMMANDER);
            			return NULL;
        		}		
			rec_buff[nread] = 0;
  			//printf("Commander id: %d received: %s\n", my_pid,rec_buff);

			strcpy(buf_copy,rec_buff);			
			data = strtok_r(buf_copy,DELIMITER,&tok);

//retrive recv_pid
				recv_pid = atoi(strtok_r(NULL,DELIMITER,&tok));
#if DEBUG==1
				LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " received: " << rec_buff << "from: " << recv_pid << "\n");
				//printf("Commander id: %d recved msg from %d\n",my_pid, recv_pid);
#endif
		
			if(strcmp(data,"PHASE2_RESPONSE") == 0)	
			{
				//expects data in the format
				//PHASE2_RESPONSE:<ACCEPTOR_ID>:<BALLOT>:

				balot_str = strtok_r(NULL,DELIMITER,&tok);
				//retrive components of the recv_ballot
				if(balot_str)
				{
					recv_ballot.bnum = atoi(strtok_r(balot_str,DELIMITER_SEC,&tok1));
					recv_ballot.leader_id = atoi(strtok_r(NULL,DELIMITER_SEC,&tok1));
				}
				else
				{
					LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " ballot str error \n");
					//printf("!!!!!!!!!Commander id:%d error in fetching ballot str %s from %s\n",my_pid,balot_str,rec_buff);
					//printf("!!!!!!!!!Commander id:%d Cannot process slot %d\n",my_pid,my_pval.slot_number);
					pthread_exit(NULL);	
				}
				if(ballot_compare(recv_ballot, my_pval.ballot) == 0)
				{
					not_accepted -= 1;

					if(not_accepted < MAX_ACCEPTORS/2)
					{
						//accepted from majority achieved - send command to all replicas
						//sending data in the format
						//DECISION:COMMANDER_ID:SLOT_NUMBER:COMMAND
						//printf("here\n");
						strcpy(send_buff,"DECISION");
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pval.slot_number);
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pval.command);
						strcat(send_buff,DELIMITER);

						if(broadcast_replicas(TALKER_COMMANDER,send_buff,replica_addr,replica_addr_len))
						{
#if DEBUG == 1
							LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " decision broadcasted \n");
							//printf("decision broadcasted at commander %d\n",my_pid);
							
#endif
							send_success(TALKER_COMMANDER,parent_id,send_buff,parent_addr,parent_addr_len);
		

						}
						else
						{
							LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " decision broadcast failed \n");			
							//printf("broadcast of decision failed at commander %d\n",my_pid);	
							pthread_exit(NULL);	
						}

						//job complete
						pthread_exit(NULL);	
					}
				}
				else
				{
					//some higher ballot has come into existence - so just give up & preempt
					//sending data in the format
					//PREEMPTED:SENDER_ID:BALLOT:

					PREPARE_PREEMPTED_MSG(send_buff, recv_ballot);
					
					if(send_preemption(TALKER_COMMANDER,parent_id,send_buff,parent_addr,parent_addr_len))
					{
#if DEBUG==1
						LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " preemption sent successfully \n");
						//printf("preemption successfully sent from commander %d\n",my_pid);
#endif						
					}
					else
					{
						LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " sending preemption failed \n");
						//printf("preemption failed at commander %d",my_pid);	
					}
					pthread_exit(NULL);

				}
			
			}
			else
			{
				LOG4CXX_TRACE(CommLogger,"Commander id: " << my_pid << " undefined msg received"  << data << "\n");
				//printf("undefined msg received at commander id: %d msg:%s\n",my_pid,data);
			}
		}

	}	
pthread_exit(NULL);
}
/*
int main()
{
return 0;
}*/
