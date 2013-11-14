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
LoggerPtr ScoutLogger(Logger::getLogger("scout"));

#define MAX_SET_SIZE 100
#define TALKER_SCOUT scout_comm.comm_fd[TALKER_INDEX]
#define LISTENER_SCOUT scout_comm.comm_fd[LISTENER_INDEX]

#define PREPARE_PREEMPTED_MSG(STR,BALLOT) BALLOT_STRING_PREP(bstr,BALLOT); \
					strcpy(STR,"PREEMPTED"); \
					strcat(STR,DELIMITER); \
					sprintf(STR,"%s%d",STR,my_pid); \
					strcat(STR,DELIMITER); \
					strcat(STR,bstr); \
					strcat(STR,DELIMITER); 


#define PVAL_STRING_PREP(STR,PVAL_SET)  strcpy(STR,""); strcpy(bstr,"");\
						for(i=0;i<pval_size;i++) \
						{ \
							BALLOT_STRING_PREP(bstr,PVAL_SET[i].ballot); \
							strcat(STR,bstr); \
							sprintf(STR,"%s%d",STR,PVAL_SET[i].slot_number); \
							strcat(STR,DELIMITER_SEC);  \
							sprintf(STR,"%s%d",STR,PVAL_SET[i].command); \
							strcat(STR,DELIMITER_SEC); \
						} 
 	
extern int ACCEPTOR_PORT_LIST[MAX_ACCEPTORS], LEADER_PORT_LIST[MAX_LEADERS] ;
extern int REPLICA_PORT_LIST[MAX_REPLICAS];
//extern int SCOUT_PORT_LIST[MAX_SCOUTS], COMMANDER_PORT_LIST[MAX_COMMANDERS];

bool send_preemption(int,int,char[],struct sockaddr, socklen_t);
int ballot_compare(struct BALLOT_NUMBER,struct BALLOT_NUMBER);

bool configure_scout(int my_pid,struct COMM_DATA *comm_scout)
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
	listener_addr.sin_port = htons(SCOUT_PORT_STARTER+my_pid); 

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
    	//printf("listener using port %d\n", ntohs(listener_addr.sin_port));

	//talker setup
	if (( talker_fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0){
			perror("talker socket ");
       			return false;
	}
	//printf("talker_fd = %d\n",talker_fd);

	talker_addr.sin_family = AF_INET;
	talker_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    	talker_addr.sin_port = htons(0);  // pick any free port

	if (bind(talker_fd, (struct sockaddr *) &talker_addr, sizeof(talker_addr)) < 0)
    	{
        perror("talker bind ");
        close(talker_fd);
        return false;
    	}   

	comm_scout->comm_fd[LISTENER_INDEX] = listener_fd;
	comm_scout->comm_fd[TALKER_INDEX] = talker_fd;

return true;
}

bool broadcast_ballot(int talker_fd,int my_pid,struct BALLOT_NUMBER my_ballot,struct sockaddr dest_addr[],socklen_t dest_addr_len[])
{
	char send_buff[BUFSIZE];
	int i,ret;
	char bstr[BUFSIZE];

	BALLOT_STRING_PREP(bstr,my_ballot);
	//sending data in the format
	//PHASE1_REQUEST:<SCOUT_ID>:<BALLOT>:
	strcpy(send_buff,"PHASE1_REQUEST");
	strcat(send_buff,DELIMITER);
	sprintf(send_buff,"%s%d",send_buff,my_pid);
	strcat(send_buff,DELIMITER);
	strcat(send_buff,bstr);
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

void* scout(void *thread_data) //acceptor list and replica list is global
{
	int not_adopted = MAX_ACCEPTORS;
	int i,pval_size;
	struct COMM_DATA scout_comm;
	struct PVAL pvalues[MAX_SET_SIZE];
//scout args
	int parent_id,my_pid;
	struct BALLOT_NUMBER my_ballot;
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
	struct sockaddr_in *acceptor_addr_in[MAX_ACCEPTORS];
	struct sockaddr acceptor_addr[MAX_ACCEPTORS];
	socklen_t acceptor_addr_len[MAX_ACCEPTORS];
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
	char *ballot_str,*data,*temp,*accepted_set_str,*tok,*tok1,*tok2;
	char bstr[BUFSIZE],pval_str[BUFSIZE];
	char buff_copy[BUFSIZE];
//thread data struct
	struct SCOUT_THREAD_ARG *args;

//configure logger
 DOMConfigurator::configure("scout_log_config.xml");
//configure scout data
	args = (struct SCOUT_THREAD_ARG*) thread_data;

	my_pid = args->my_pid;
	parent_id = args->parent_id;
	my_ballot = args->my_ballot;
//hostname configuration
	gethostname(hostname, sizeof(hostname));
	hp = gethostbyname(hostname);
	if (hp == NULL) 
	{ 
		LOG4CXX_TRACE(ScoutLogger,hostname << " : unknown host\n");
		return NULL; 
	} 

//configure sender and listener
	if(configure_scout(my_pid,&scout_comm))
	{
		LOG4CXX_TRACE(ScoutLogger,"Scout id: " << my_pid << " configured successfully\n");
		//printf("Scout id: %d configured successfully\n",my_pid);
	}
	else
	{
		LOG4CXX_TRACE(ScoutLogger,"Scout id: " << my_pid << " config error \n");
		//printf("Error in config of scout id: %d\n",my_pid);
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

//setup parent address	(will be used for preemption
	parent_addr_in = (struct sockaddr_in *)&(parent_addr);
	parent_addr_in->sin_family = AF_INET;
	memcpy(&parent_addr_in->sin_addr, hp->h_addr, hp->h_length); 
	parent_addr_in->sin_port  = htons(LEADER_PORT_LIST[parent_id]);  
	parent_addr_len = sizeof(parent_addr);
//send proposal to all acceptors
	if(broadcast_ballot(TALKER_SCOUT,my_pid,my_ballot,acceptor_addr,acceptor_addr_len))
	{
#if DEBUG == 1
		//LOG4CXX_TRACE(ScoutLogger,"Scout id: " << my_pid << " pval broadcasted \n");
	//	printf("pval broadcasted at scout %d\n",my_pid);
#endif
	}
	else
	{
		LOG4CXX_TRACE(ScoutLogger,"Scout id: " << my_pid << " pval broadcast failed \n");
		//printf("broadcast of received pval failed at scout %d\n",my_pid);	
		return NULL;	
	}

//wait for reply

	while(1)
	{
		maxfd = LISTENER_SCOUT+1;
		FD_ZERO(&readfds); 
		FD_SET(LISTENER_SCOUT, &readfds);

		ret = select(maxfd, &readfds, NULL, NULL, NULL);  //blocks forever till it receives a message


		if(ret <0)
	   	{ 
			LOG4CXX_TRACE(ScoutLogger,"Scout id: " << my_pid << " select error \n");
	     		//printf("\nSelect error\n");   
	     		return NULL;
	   	} 

		if(FD_ISSET (LISTENER_SCOUT, &readfds))
		{
			temp_paddr_len = sizeof(temp_paddr);
			nread = recvfrom (LISTENER_SCOUT, recv_buff, BUFSIZE, 0, 
               	       			(struct sockaddr *)&temp_paddr, &temp_paddr_len); 
		
		 	if (nread < 0)
		       	{
				LOG4CXX_TRACE(ScoutLogger,"Scout id: " << my_pid << " recv from failed \n");
		        	perror("recvfrom ");
            			close(LISTENER_SCOUT);
            			return NULL;
        		}		
			recv_buff[nread] = 0;
			//printf("received: %s\n", recv_buff);

			strcpy(buff_copy,recv_buff);			
			data = strtok_r(buff_copy,DELIMITER,&tok);

//retrive recv_pid
				recv_pid = atoi(strtok_r(NULL,DELIMITER,&tok));
#if DEBUG==1
				//LOG4CXX_TRACE(ScoutLogger,"Scout id: " << my_pid << " received: " << recv_buff << "from: " << recv_pid << "\n");
				//printf("recved msg from %d\n",recv_pid);
#endif
			if(strcmp(data,"PHASE1_RESPONSE") == 0)	
			{
				//expects data in the format
				//PHASE1_RESPONSE:<ACCEPTOR_ID>:<BALLOT>:<ACCEPTED_SET>:

				ballot_str = strtok_r(NULL,DELIMITER,&tok);
				
				accepted_set_str = strtok_r(NULL,DELIMITER,&tok);
				
				//retrive components of accepted set str and update

				i=0;pval_size=0;
				temp = strtok_r(accepted_set_str,DELIMITER_SEC,&tok1);
				while(temp != NULL)
				{	
#if DEBUG==1

						//printf("!!!!scout id %d temp:%s\n",my_pid,temp);
#endif

						pvalues[i].ballot.bnum = atoi(temp);
						temp =  strtok_r(NULL,DELIMITER_SEC,&tok1);
						pvalues[i].ballot.leader_id = atoi(temp);
						temp =  strtok_r(NULL,DELIMITER_SEC,&tok1);
						pvalues[i].slot_number =  atoi(temp);
						temp =  strtok_r(NULL,DELIMITER_SEC,&tok1);
						pvalues[i].command =  atoi(temp);
						temp = strtok_r(NULL,DELIMITER_SEC,&tok1);				

					i++;
					pval_size++;
				}

				//retrive components of the recv_ballot
				recv_ballot.bnum = atoi(strtok_r(ballot_str,DELIMITER_SEC,&tok2));
				recv_ballot.leader_id = atoi(strtok_r(NULL,DELIMITER_SEC,&tok2));



				//compare ballot and take action accordingly
				if(ballot_compare(recv_ballot, my_ballot) == 0)
				{
					not_adopted -= 1;

					if(not_adopted < MAX_ACCEPTORS*1.0/2.0)
					{
						//accepted from majority achieved - send command to all replicas
						//sending data in the format
						//ADOPTED:SCOUT_ID:BALLOT:PVALS:
						strcpy(send_buff,"ADOPTED");
						strcat(send_buff,DELIMITER);
						sprintf(send_buff,"%s%d",send_buff,my_pid);
						strcat(send_buff,DELIMITER);
						BALLOT_STRING_PREP(bstr,my_ballot);
						strcat(send_buff,bstr);
						strcat(send_buff,DELIMITER);
						PVAL_STRING_PREP(pval_str,pvalues);
						strcat(send_buff,pval_str);
						strcat(send_buff,DELIMITER);
						
						//send ADOPTED
						ret = sendto(TALKER_SCOUT, send_buff, strlen(send_buff), 0, 
      							(struct sockaddr *)&parent_addr, parent_addr_len);
			
						if (ret < 0)
     						{
				      			perror("sendto ");
		        				close(TALKER_SCOUT);
				      			return NULL;
     						}
#if DEBUG==1

						//LOG4CXX_TRACE(ScoutLogger,"Scout id: " << my_pid << " adopted msg sent" << "\n");
						//printf("adopted successfully sent from scout %d\n",my_pid);
#endif	
						//job complete
						return NULL;	
					}

				}
				else
				{

					//some higher ballot has come into existence - so just give up & preempt
					//sending data in the format
					//PREEMPTED:SENDER_ID:BALLOT:

					PREPARE_PREEMPTED_MSG(send_buff, recv_ballot);
					
					if(send_preemption(TALKER_SCOUT,parent_id,send_buff,parent_addr,parent_addr_len))
					{
#if DEBUG==1
						//LOG4CXX_TRACE(ScoutLogger,"Scout id: " << my_pid << " preemption msg sent" << "\n");
						//printf("preemption successfully sent from scout %d\n",my_pid);
#endif						
					}
					else
					{
						//LOG4CXX_TRACE(ScoutLogger,"Scout id: " << my_pid << " preemption msg sending failed" << "\n");
						//printf("preemption failed at scout %d",my_pid);	
					}
					return NULL;

				}

				

			}
		}

	}
pthread_exit(NULL);
}
