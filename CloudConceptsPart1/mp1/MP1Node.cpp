/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        cout << "failed" << endl;
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    //cout << "memberNode " << memberNode->addr.getAddress() << " alive at time " <<  memberNode->heartbeat << endl;

    if (memberNode->bFailed) {
        cout << "failed" << endl;
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	//get message type
    MessageHdr *msgHeader = ((MessageHdr *) data);
    MsgTypes type = msgHeader->msgType;
    if (type == JOINREQ) {
        cout << "received JOINREQ" << endl;
        Address address = getAddressFromMem(data + sizeof(MessageHdr));
        int id = getIdFromAddress(address);
        short port = getPortFromAddress(address);
        long heartbeat = getHeartbeatFromMem(data + sizeof(MessageHdr) + sizeof(address) + 1);
        MemberListEntry memberListEntry(id, port, heartbeat, heartbeat);
        MessageHdr *msg;
        //allocate message
        size_t msgsize = sizeof(MessageHdr) + (sizeof(memberNode->addr.addr) + sizeof(long) + 1) + ((sizeof(int) + sizeof(short) + 1 + sizeof(long)) * (memberNode->memberList.size()));
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        msg->msgType = JOINREP;
        char* pointer = (char*)(msg+1);
        memcpy(pointer, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        pointer += (1 + sizeof(memberNode->addr.addr));
        memcpy((char*)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        pointer += sizeof(long);
        for (int i = 0; i < memberNode->memberList.size(); i++){
            MemberListEntry member = memberNode->memberList.at(i);
            Address sendAddr = initAddressFromIdPort(member.id, member.port);

            MessageHdr *testmsg;
            size_t testmsgsize = sizeof(MessageHdr) + sizeof(address.addr) + sizeof(long) + 1;
            testmsg = (MessageHdr *) malloc(testmsgsize * sizeof(char));
            testmsg->msgType = NEWMEM;
            memcpy((char *)(testmsg+1), &address.addr, sizeof(address.addr));
            memcpy((char *)(testmsg+1) + 1 + sizeof(address.addr), &heartbeat, sizeof(long));
            emulNet->ENsend(&memberNode->addr, &sendAddr, (char *)testmsg, testmsgsize);
            free(testmsg);

            memcpy(pointer, &member.id, sizeof(int));
            pointer += sizeof(int);
            memcpy(pointer, &member.port, sizeof(short));
            pointer += (1 + sizeof(short));
            memcpy(pointer, &member.heartbeat, sizeof(long));
            pointer += sizeof(long);
        }
        emulNet->ENsend(&memberNode->addr, &address, (char *)msg, msgsize);
        free(msg);

        memberNode->memberList.push_back(memberListEntry);
        log->logNodeAdd(&memberNode->addr, &address);

    } else if (type == JOINREP) {
        cout << "received JOINREP" << endl;
        char* pointer = data + sizeof(MessageHdr);
        Address address = getAddressFromMem(pointer);
        pointer += (sizeof(address) + 1);
        int id = getIdFromAddress(address);
        short port = getPortFromAddress(address);
        long heartbeat = getHeartbeatFromMem(pointer);
        pointer += sizeof(long);
        //TODO add time
        MemberListEntry memberListEntry(id, port, heartbeat, 0);
        memberNode->memberList.push_back(memberListEntry);
        log->logNodeAdd(&memberNode->addr, &address);
        int remainingDataSize = ((data + size) - pointer);
        int listSize = 0;
        if (remainingDataSize > 0) {
            listSize = remainingDataSize / (sizeof(int) + sizeof(short) + 1 + sizeof(long));
        }
        for (int i = 0; i < listSize; i++) {
            int id = 0;
            memcpy(&id, pointer, sizeof(int));
            pointer += sizeof(int);
            short port = 0;
            memcpy(&port, pointer, sizeof(short));
            pointer += (1 + sizeof(short));
            long heartbeat = 0;
            memcpy(&heartbeat, pointer, sizeof(long));
            pointer += sizeof(long);
            MemberListEntry memberListEntry(id, port, heartbeat, 0);
            memberNode->memberList.push_back(memberListEntry);
            Address memberAddress = initAddressFromIdPort(id, port);
            log->logNodeAdd(&memberNode->addr, &memberAddress);
        }
        memberNode->inGroup = true;
        
    } else if (type == NEWMEM) {
        cout << "received NEWMEM" << endl;
        Address address = getAddressFromMem(data + sizeof(MessageHdr));
        int id = getIdFromAddress(address);
        short port = getPortFromAddress(address);
        long heartbeat = getHeartbeatFromMem(data + sizeof(MessageHdr) + sizeof(address) + 1);
        MemberListEntry memberListEntry(id, port, heartbeat, heartbeat);
        memberNode->memberList.push_back(memberListEntry);
        log->logNodeAdd(&memberNode->addr, &address);

    } else if (type == HEART) {
        //cout << "received HEART" << endl;
        Address address = getAddressFromMem(data + sizeof(MessageHdr));
        int id = getIdFromAddress(address);
        short port = getPortFromAddress(address);
        long heartbeat = getHeartbeatFromMem(data + sizeof(MessageHdr) + sizeof(address) + 1);
        //find member with id and update heartbeat
        for (int i = 0; i < memberNode->memberList.size(); i++){
            MemberListEntry member = memberNode->memberList.at(i);
            if (member.id == id) {
                memberNode->memberList.at(i).heartbeat = heartbeat;
                //cout << memberNode->addr.getAddress() << " updated heartbeat of " << address.getAddress() << " with heartbeat " << heartbeat << endl;
            }
        }
        
    } else {
        cout << "received UNKNOWN" << endl;
    }
} 

Address MP1Node::getAddressFromMem(char* data) {
    Address address;
    address.init();
    memcpy(&address, data, sizeof(address));
    //cout << "address: " << address.getAddress() << endl;
    return address;
}

long MP1Node::getHeartbeatFromMem(char* data) {
    long heartbeat;
    memcpy(&heartbeat, data, sizeof(long));
    //cout << "heartbeat: " << heartbeat << endl;
    return heartbeat;
}

int MP1Node::getIdFromAddress(Address address) {
    int id = 0;
    memcpy(&id, &address, sizeof(int));
    //cout << "id: " << id << endl;
    return id;
}

short MP1Node::getPortFromAddress(Address address) {
    short port = 0;
    char* pointer = (char *)&address;
    memcpy(&port, pointer + sizeof(int), sizeof(short));
    //cout << "port: " << port << endl;
    return port;
}

Address MP1Node::initAddressFromIdPort(int id, short port) {
    Address address;
    address.init();
    memcpy(&address, &id, sizeof(int));
    memcpy(&address + sizeof(short), &port, sizeof(short));
    //cout << "address: " << address.getAddress() << endl;
    return address;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    //cout << "memberNode " << memberNode->addr.getAddress() << " alive at time " <<  memberNode->heartbeat << endl;

    memberNode->heartbeat++;

    vector<int> removeList;

    for (int i = 0; i < memberNode->memberList.size(); i++){
            MemberListEntry member = memberNode->memberList.at(i);

            //send heartbeat to all member entries
            Address sendAddr = initAddressFromIdPort(member.id, member.port);
            MessageHdr *testmsg;
            size_t testmsgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long) + 1;
            testmsg = (MessageHdr *) malloc(testmsgsize * sizeof(char));
            testmsg->msgType = HEART;
            memcpy((char *)(testmsg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
            memcpy((char *)(testmsg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
            emulNet->ENsend(&memberNode->addr, &sendAddr, (char *)testmsg, testmsgsize);
            free(testmsg);

            //cout << memberNode->addr.getAddress() << " checking heartbeat of " << sendAddr.getAddress() << " with heartbeat " << member.heartbeat << " and timestamp " << member.timestamp << endl;

            //check health of each member entry
            if ((member.timestamp - member.heartbeat) >= TFAIL) {
                log->logNodeRemove(&memberNode->addr, &sendAddr);
                //mark to be removed
                removeList.push_back(member.id);
                cout << "remove member: " << sendAddr.getAddress() << endl;

            } else {
                //increment time of each member entry
                memberNode->memberList.at(i).timestamp++;
            }
    }

    for (int i = 0; i < removeList.size(); i++) {
        for (int j = 0; j < memberNode->memberList.size(); j ++) {
            if (memberNode->memberList.at(j).id == removeList.at(i)) {
                memberNode->memberList.erase(memberNode->memberList.begin() + j);
                break;
            }
        }
    }

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
