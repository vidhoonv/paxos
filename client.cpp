#include<stdio.h>
#include<pthread.h>
#include "pax_types.h"

/* this is a stub client to test message passsing and basic paxos */

#define TALKER client_comm.comm_fd[TALKER_INDEX]
#define LISTENER client_comm.comm_fd[LISTENER_INDEX]

int ACCEPTOR_PORT_LIST[MAX_ACCEPTORS] = {3000,3002,3004};//,3006,3008,3010,3012,3014,3016,3018};
int LEADER_PORT_LIST[MAX_LEADERS] = {4000,4002};
int REPLICA_PORT_LIST[MAX_REPLICAS] = {2000,2002};
int COMMANDER_PORT_LIST[MAX_COMMANDERS] = {5000,5001,5002,5003,5004,5005,5006,5007,5008,5009,5010,5011,5012,5013,5014,5015,5016,5017,5018,5019,5020,5021,5022,5023,5024,5025,5026,5027,5028,5029,5030,5031,5032,5033,5034,5035,5036,5037,5038,5039,5040,5041,5042,5043,5044,5045,5046,5047,5048,5049,5050,5051,5052,5053,5054,5055,5056,5057,5058,5059};
int SCOUT_PORT_LIST[MAX_SCOUTS] = {6000,6001,6002,6003,6004,6005,6006,6007,6008,6009,6010,6011,6012,6013,6014,6015,6016,6017,6018,6019,6020,6021,6022,6023,6024,6025,6026,6027,6028,6029,6030,6031,6032,6033,6034,6035,6036,6037,6038,6039,6040,6041,6042,6043,6044,6045,6046,6047,6048,6049,6060,6051,6052,6053,6054,6055,6056,6057,6058,6059};
int CLIENT_PORT_LIST[MAX_CLIENTS] = {7000};


struct COMM_DATA client_comm;

bool configure_client(int my_pid,struct COMM_DATA *comm_client)
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
	listener_addr.sin_port = htons(CLIENT_PORT_LIST[my_pid]); 

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

	comm_client->comm_fd[LISTENER_INDEX] = listener_fd;
	comm_client->comm_fd[TALKER_INDEX] = talker_fd;

return true;
}

void* listener(void *arg)
{
//comm listening variables
	fd_set readfds;
	int maxfd;
	char recv_buff[BUFSIZE];
	int nread=0;

//comm common
	struct sockaddr_storage temp_paddr;
	socklen_t temp_paddr_len;

//misc
	int i,ret=0,recv_pid;
	char buff_copy[BUFSIZE];
	char *data;

	while(1)
	{
		maxfd = LISTENER+1;
		FD_ZERO(&readfds); 
		FD_SET(LISTENER, &readfds);

		ret = select(maxfd, &readfds, NULL, NULL, NULL);  //blocks forever till it receives a message


		if(ret <0)
	   	{ 
	     		printf("\nSelect error\n");   
	     		return NULL;
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
            			return NULL;
        		}		
			recv_buff[nread] = 0;
  			//printf("received: %s\n", recv_buff);

			strcpy(buff_copy,recv_buff);			
			data = strtok(buff_copy,DELIMITER);

#if DEBUG==1
				printf("recved msg from replica content:%s\n",recv_buff);
#endif
			
	
		}
	}
}
int main(int argc, char **argv)
{
//command sequences for testing
	//int command_seq1[3] = {3,2,1};
	//int command_seq2[3] = {2,1,3};

	int command_seq1[5] = {3,2,1,5,4};
	int command_seq2[5] = {2,1,4,3,5};

//int command_seq1[15] = {8,3,7,5,9,2,4,1,6,10,13,15,11,14,12};
//int command_seq2[15] = {6,1,3,8,9,5,7,4,2,15,12,11,13,10,14};


	
	
	int my_pid;
	pthread_t listener_thread;



//comm related variables
	struct hostent *hp;
	char hostname[64];

	


//comm sending variables
	char send_buff[BUFSIZE];
	struct sockaddr_in *replica_addr_in[MAX_REPLICAS],*process_addr_in;
	struct sockaddr replica_addr[MAX_REPLICAS],process_addr;
	socklen_t replica_addr_len[MAX_REPLICAS],process_addr_len;

	int i,j,ret =0;


//check runtime arguments
	if(argc!=2)
	{
		printf("Usage: ./client <client_id>\n");
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

//setup replica addresses

	for(i=0;i<MAX_REPLICAS;i++)
	{
		replica_addr_in[i] = (struct sockaddr_in *)&(replica_addr[i]);
		replica_addr_in[i]->sin_family = AF_INET;
		memcpy(&(replica_addr_in[i]->sin_addr), hp->h_addr, hp->h_length); 
		replica_addr_in[i]->sin_port  = htons(REPLICA_PORT_LIST[i]);  
		replica_addr_len[i] = sizeof(replica_addr[i]);
	}


	//configure client talker and listener ports	
	//setup the client
	if(configure_client(my_pid,&client_comm))
	{
		printf("Client id: %d configured successfully\n",my_pid);
	}
	else
	{
		printf("Error in config of client id: %d\n",my_pid);
		return -1;
	}

//create listener thread

pthread_create(&listener_thread,NULL,listener,NULL);	


//send commands to replicas in different order for testing
for(j=0;j<MAX_REPLICAS;j++)
{
	for(i=0;i<5;i++)
	{
	
		strcpy(send_buff,"REQUEST:0:");
		if(j==0)
			sprintf(send_buff,"%s%d:",send_buff,command_seq1[i]);
		else if(j==1)
			sprintf(send_buff,"%s%d:",send_buff,command_seq2[i]);
		ret = sendto(TALKER, send_buff, strlen(send_buff), 0, 
      			(struct sockaddr *)&replica_addr[j], replica_addr_len[j]);
			
		if (ret < 0)
     		{
      			perror("sendto ");
		        close(TALKER);
     		}
	}
}
pthread_join(listener_thread,NULL);
return 0;
}
