/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
	pendingTransactions = new map<int, int>();
	pendingMessages = new map<int, MessageArchive>();
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
	delete pendingTransactions;
	delete pendingMessages;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();
	curMemList.emplace_back(Node(this->memberNode->addr));

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	vector<Node> oldMemList = ring;
	ring = curMemList;

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if (oldMemList.size() != ring.size()) {
		stabilizationProtocol(oldMemList);
	
		// Vector holding the next two neighbors in the ring who have my replicas
		hasMyReplicas.clear();
		vector<Node>::iterator nextIt = ring.begin();
		while(nextIt != ring.end()) {
			if (nextIt->nodeAddress == this->memberNode->addr){
				while(hasMyReplicas.size() < 2) {
					nextIt++;
					if (nextIt == ring.end()) {
						nextIt = ring.begin();
					}
					hasMyReplicas.emplace_back(*nextIt);
				}
				break;
			}
			nextIt++;
		}
		// Vector holding the previous two neighbors in the ring whose replicas I have
		haveReplicasOf.clear();
		vector<Node>::reverse_iterator prevIt = ring.rbegin();
		while(prevIt != ring.rend()) {
			if (prevIt->nodeAddress == this->memberNode->addr){
				while(haveReplicasOf.size() < 2) {
					prevIt++;
					if (prevIt == ring.rend()) {
						prevIt = ring.rbegin();
					}
					haveReplicasOf.emplace_back(*prevIt); 
				}
				break;
			}
			prevIt++;
		}

		/*cout << "hasMyReplicas" << endl;
		for (Node node : hasMyReplicas) {
			cout << node.nodeAddress.getAddress() << endl;
		}*/

		/*cout << "haveReplicasOf" << endl;
		for (Node node : haveReplicasOf) {
			cout << node.nodeAddress.getAddress() << endl;
		}*/

	}
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	cout << "client create called on memberNode: " << this->memberNode->addr.getAddress() << " for key: " << key << " for value: " << value << endl;
	vector<Node> nodes = findNodes(key);
	for (Node node : nodes) {
		Message message(g_transID, this->memberNode->addr, CREATE, key, value);
		this->emulNet->ENsend(&this->memberNode->addr, &node.nodeAddress, message.toString());
	}
	pendingTransactions->emplace(g_transID, 0);
	MessageArchive messageArchive(this->par->getcurrtime(), CREATE, key, value);
	pendingMessages->emplace(g_transID, messageArchive);
	g_transID++;
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	cout << "client read called on memberNode: " << this->memberNode->addr.getAddress() << " for key: " << key << endl;
	vector<Node> nodes = findNodes(key);
	for (Node node : nodes) {
		Message message(g_transID, this->memberNode->addr, READ, key);
		this->emulNet->ENsend(&this->memberNode->addr, &node.nodeAddress, message.toString());
	}
	pendingTransactions->emplace(g_transID, 0);
	MessageArchive messageArchive(this->par->getcurrtime(), READ, key, "");
	pendingMessages->emplace(g_transID, messageArchive);
	g_transID++;
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	cout << "client update called on memberNode: " << this->memberNode->addr.getAddress() << " for key: " << key << endl;
	vector<Node> nodes = findNodes(key);
	for (Node node : nodes) {
		Message message(g_transID, this->memberNode->addr, UPDATE, key, value);
		this->emulNet->ENsend(&this->memberNode->addr, &node.nodeAddress, message.toString());
	}
	pendingTransactions->emplace(g_transID, 0);
	MessageArchive messageArchive(this->par->getcurrtime(), UPDATE, key, value);
	pendingMessages->emplace(g_transID, messageArchive);
	g_transID++;
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	cout << "client delete called on memberNode: " << this->memberNode->addr.getAddress() << " for key: " << key << endl;
	vector<Node> nodes = findNodes(key);
	for (Node node : nodes) {
		Message message(g_transID, this->memberNode->addr, DELETE, key);
		this->emulNet->ENsend(&this->memberNode->addr, &node.nodeAddress, message.toString());
	}
	pendingTransactions->emplace(g_transID, 0);
	MessageArchive messageArchive(this->par->getcurrtime(), DELETE, key, "");
	pendingMessages->emplace(g_transID, messageArchive);
	g_transID++;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	
	return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string messageStr(data, data + size);
		Message message(messageStr);

		/*
		 * Handle the message types here
		 */
		
		switch (message.type) {
			case CREATE: {
				//cout << "CREATE MESSAGE RECEIVED" << endl;
				bool result = createKeyValue(message.key, message.value, PRIMARY);
				if (result) {
					this->log->logCreateSuccess(&this->memberNode->addr, false, message.transID, message.key, message.value);
				} else {
					this->log->logCreateFail(&this->memberNode->addr, false, message.transID, message.key, message.value);
				}
				Message response(message.transID, this->memberNode->addr, REPLY, result);
				this->emulNet->ENsend(&this->memberNode->addr, &message.fromAddr, response.toString());
				break;
			}
			case READ: {
				//cout << "READ MESSAGE RECEIVED" << endl;
				string result = readKey(message.key);
				if (result != "") {
					this->log->logReadSuccess(&this->memberNode->addr, false, message.transID, message.key, result);
				} else {
					this->log->logReadFail(&this->memberNode->addr, false, message.transID, message.key);
				}
				Message response(message.transID, this->memberNode->addr, result);
				this->emulNet->ENsend(&this->memberNode->addr, &message.fromAddr, response.toString());
				break;
			}
			case UPDATE: {
				//cout << "UPDATE MESSAGE RECEIVED" << endl;
				bool result = updateKeyValue(message.key, message.value, PRIMARY);
				if (result) {
					this->log->logUpdateSuccess(&this->memberNode->addr, false, message.transID, message.key, message.value);
				} else {
					this->log->logUpdateFail(&this->memberNode->addr, false, message.transID, message.key, message.value);
				}
				Message response(message.transID, this->memberNode->addr, REPLY, result);
				this->emulNet->ENsend(&this->memberNode->addr, &message.fromAddr, response.toString());
				break;
			}
			case DELETE: {
				//cout << "DELETE MESSAGE RECEIVED" << endl;
				bool result = deletekey(message.key);
				if (result) {
					this->log->logDeleteSuccess(&this->memberNode->addr, false, message.transID, message.key);
				} else {
					this->log->logDeleteFail(&this->memberNode->addr, false, message.transID, message.key);
				}
				Message response(message.transID, this->memberNode->addr, REPLY, result);
				this->emulNet->ENsend(&this->memberNode->addr, &message.fromAddr, response.toString());
				break;
			}
			case REPLY: {
				//cout << "REPLY MESSAGE RECEIVED" << endl;
				if (pendingTransactions->find(message.transID) != pendingTransactions->end()) {
					int replies = pendingTransactions->at(message.transID);
					replies++;
					if (replies < 2) {
						pendingTransactions->at(message.transID) = replies;
					} else {
						pendingTransactions->erase(message.transID);
						MessageArchive archive = pendingMessages->at(message.transID);
						switch (archive.type) {
							case CREATE:
								this->log->logCreateSuccess(&this->memberNode->addr, true, message.transID, archive.key, archive.value);
								break;
							case UPDATE:
								if (message.success) {
									this->log->logUpdateSuccess(&this->memberNode->addr, true, message.transID, archive.key, archive.value);
								} else {
									this->log->logUpdateFail(&this->memberNode->addr, true, message.transID, archive.key, archive.value);
								}
								break;
							case DELETE:
								if (message.success) {
									this->log->logDeleteSuccess(&this->memberNode->addr, true, message.transID, archive.key);
								} else {
									this->log->logDeleteFail(&this->memberNode->addr, true, message.transID, archive.key);
								}
								break;
						}
						pendingMessages->erase(message.transID);
					}
				}
				break;
			}
			case READREPLY:
				//cout << "READREPLY MESSAGE RECEIVED" << endl;
				if (pendingTransactions->find(message.transID) != pendingTransactions->end()) {
					int replies = pendingTransactions->at(message.transID);
					replies++;
					if (replies < 2) {
						pendingTransactions->at(message.transID) = replies;
					} else {
						pendingTransactions->erase(message.transID);
						MessageArchive archive = pendingMessages->at(message.transID);
						if (message.value != "") {
							this->log->logReadSuccess(&this->memberNode->addr, true, message.transID, archive.key, message.value);
						} else {
							this->log->logReadFail(&this->memberNode->addr, true, message.transID, archive.key);
						}
						pendingMessages->erase(message.transID);
					}
				}
				break;
		}
	}

	// check for unresponsive pending transactions
	vector<int> toRemove;
	for (auto const& pending : *pendingMessages) {
		if ((pending.second.timestamp + 10) < this->par->getcurrtime()) {
			toRemove.emplace_back(pending.first);
			switch (pending.second.type) {
				case CREATE:
					this->log->logCreateFail(&this->memberNode->addr, true, pending.first, pending.second.key, pending.second.value);
					break;
				case READ:
					this->log->logReadFail(&this->memberNode->addr, true, pending.first, pending.second.key);
					break;
				case UPDATE:
					this->log->logUpdateFail(&this->memberNode->addr, true, pending.first, pending.second.key, pending.second.value);
					break;
				case DELETE:
					this->log->logDeleteFail(&this->memberNode->addr, true, pending.first, pending.second.key);
					break;
			}
		}
	}

	// remove failed pending transactions
	for (auto const& remove : toRemove) {
		pendingTransactions->erase(remove);
		pendingMessages->erase(remove);
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol(const vector<Node> &oldRing) {
	
	cout << "stabilizing ring on " << this->memberNode->addr.getAddress() << endl;
	//only consider failures and not adds
	if (oldRing.size() < ring.size()) {
		cout << "currently not stabilizing ring additions" << endl;
		return;
	}
	//find missing nodes
	vector<Node> missing;
	vector<Node>::iterator it = ring.begin();
	for (Node node : oldRing) {
		//cout << node.nodeAddress.getAddress() << endl;
		if (it != ring.end() && node.nodeAddress == it->nodeAddress) {
			it++;
		} else {
			cout << "missing node " << node.nodeAddress.getAddress() << endl;
			missing.emplace_back(node);
		}
	}
	//if missing node(s) matches hasMyReplicas then recreate all values in hash table accross system
	for (Node missingNode : missing) {
		for (Node hasMyReplicasNode : hasMyReplicas) {
			if (missingNode.nodeAddress == hasMyReplicasNode.nodeAddress) {
				//iterate accross hash table and re-initialize all keys
				for (auto const& values : ht->hashTable) {
					clientCreate(values.first, values.second);
				}
			}
		}
	}
	//if missing node(s) matches haveReplicasOf then recreate all values in hash table accross system
	for (Node missingNode : missing) {
		for (Node haveReplicasOfNode : haveReplicasOf) {
			if (missingNode.nodeAddress == haveReplicasOfNode.nodeAddress) {
				//iterate accross hash table and re-initialize all keys
				for (auto const& values : ht->hashTable) {
					clientCreate(values.first, values.second);
				}
			}
		}
	}

}
