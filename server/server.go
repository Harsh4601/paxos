package main

import (
	"encoding/gob"
	"fmt"
	"log"

	// "math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

func init() {

	gob.Register(PrepareMessage{})
	gob.Register(PromiseMessage{})
	gob.Register(ResponseMessage{})
	gob.Register(Transaction{})
	gob.Register(BallotPair{})
	gob.Register(Server{})
	gob.Register(AcceptMessage{})
	gob.Register(AcceptedMessage{})
	gob.Register(CommitMessage{})
	gob.Register(FetchDataStoreRequest{})
	gob.Register(FetchDataStoreResponse{})

}

type ResponseMessage struct {
	Acknowledged bool
}

type BallotPair struct {
	BallotNumber int
	ServerID     int
}

type Transaction struct {
	Sender   string
	Receiver string
	Amount   int
}

type ActiveStatus struct {
	ActiveServers map[string]bool
}

type Server struct {
	Name                  string
	Port                  string
	ServerID              int
	Balances              int
	BallotPair            BallotPair
	AcceptNum             BallotPair
	AcceptVal             []Transaction
	LocalTxn              []Transaction
	PromisesReceived      int
	acceptMessagesLock    sync.Mutex
	acceptedCount         int
	balanceLock           sync.Mutex
	ExecutedTxn           []Transaction
	PendingTxn            []Transaction
	ActiveStatus          map[string]bool
	ActiveCount           int
	DataStore             [][]Transaction
	Mutex                 sync.Mutex
	highest_len_datastore int
	Highest_Len_Server_ID int
	latency               time.Duration
	txn_executed          int
}

type PrepareMessage struct {
	BallotPair   BallotPair // Ballot number and Server ID
	Sender       string     // Name of the leader server
	SenderID     int        // Server ID of the leader
	LenDataStore int
}

type PromiseMessage struct {
	Ack          bool
	BallotPair   BallotPair
	AcceptNum    BallotPair
	AcceptVal    []Transaction
	LocalTxn     []Transaction
	ServerId     int
	LenDataStore int
}

type AcceptMessage struct {
	BallotPair BallotPair
	MajorBlock []Transaction
}

type AcceptedMessage struct {
	BallotPair BallotPair
	MajorBlock []Transaction
}
type CommitMessage struct {
	// BallotPair BallotPair
	FinalBlock []Transaction
	Balances   map[string]int
}

type FetchDataStoreRequest struct {
	ServerID     int
	LenDatastore int
}
type FetchDataStoreResponse struct {
	PendingTransaction [][]Transaction
}

type Args struct {
	Transaction Transaction
}

func (s *Server) ProcessTransaction(args *Args, reply *string) error {

	fmt.Printf("%s processing transaction: %v\n", s.Name, args.Transaction)
	// *reply = "Transaction processed"
	return nil
}

func (s *Server) startServer() {

	rpc.Register(s)

	ln, err := net.Listen("tcp", ":"+s.Port)
	if err != nil {
		log.Fatalf("Error starting the server %s on port %s: %v\n", s.Name, s.Port, err)
	}
	defer ln.Close()
	fmt.Printf("%s server running on port %s\n", s.Name, s.Port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		//DEBUG LINE--------------
		// fmt.Println("Connection accepted from:", conn.RemoteAddr())
		go rpc.ServeConn(conn)
	}
}

func (s *Server) HandleActiveStatus(activeStatus ActiveStatus, reply *string) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// log.Printf("Received active status: %+v", activeStatus.ActiveServers)

	s.ActiveStatus = activeStatus.ActiveServers

	s.ActiveCount = 0
	for _, isActive := range s.ActiveStatus {
		if isActive {
			s.ActiveCount++
		}
	}

	// log.Printf("Total number of active servers: %d", s.ActiveCount)

	return nil
}

func (s *Server) CheckBalance(activeStatus string, reply *int) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	*reply = s.Balances
	return nil
}
func (s *Server) CheckLog(activeStatus string, reply *[]Transaction) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	*reply = s.LocalTxn
	return nil
}
func (s *Server) CheckDatastore(activeStatus string, reply *[][]Transaction) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	*reply = s.DataStore
	return nil
}
func (s *Server) SendPerformance(activeStatus string, reply *[]float64) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	*reply = []float64{float64(s.txn_executed), float64(s.latency.Seconds())}
	return nil
}

func (s *Server) HandleTransaction(transaction Transaction, reply *string) error {

	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	time_start := time.Now()
	// if len(s.PendingTxn) > 0 {
	// 	s.processPendingTransactions()
	// }

	if s.Balances < transaction.Amount {
		fmt.Println("Transaction not processed, insufficient funds. Initiating Paxos.")

		// backoffTime := time.Duration(rand.Intn(1000)) * time.Millisecond
		// fmt.Printf("Server %s will wait for %v before initiating Paxos.\n", s.Name, backoffTime)

		// time.Sleep(backoffTime)

		// Check if it still needs to initiate Paxos after the backoff
		s.PendingTxn = append(s.PendingTxn, transaction)
		s.initiatePaxos()
		s.latency += time.Since(time_start)
	} else {

		s.Balances -= transaction.Amount
		s.LocalTxn = append(s.LocalTxn, transaction)

		log.Printf("Server %s: Processing transaction: %s sends %d to %s", s.Name, transaction.Sender, transaction.Amount, transaction.Receiver)
		log.Printf("New Balance for Server %s: %d", s.Name, s.Balances)
		s.latency += time.Since(time_start)
		s.txn_executed++

	}
	// // *reply = "Transaction processed"
	return nil
}

var minorBlock []Transaction

func (s *Server) initiatePaxos() {

	// s.Mutex.Lock()
	// defer s.Mutex.Unlock()
	log.Print(len(s.DataStore))
	s.BallotPair.BallotNumber++
	s.PromisesReceived = 0
	s.acceptedCount = 0
	prepareMsg := PrepareMessage{
		BallotPair: BallotPair{
			BallotNumber: s.BallotPair.BallotNumber, // 1
			ServerID:     s.ServerID,
		},
		Sender:       s.Name,
		SenderID:     s.ServerID,
		LenDataStore: len(s.DataStore),
	}
	prepareMsg.LenDataStore = len(s.DataStore)
	minorBlock = nil
	// Sending the Prepare message to other servers
	fmt.Printf("Server %s Sent Prepare Message: %v\n", s.Name, prepareMsg)
	for id := 1; id <= 5; id++ {
		if id != s.ServerID { // Skip sending to itself
			addr := fmt.Sprintf("localhost:800%d", id) 
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				fmt.Printf("Error connecting to server %d: %v\n", id, err)
				continue
			}
			defer client.Close()

			var promiseMessage PromiseMessage
			err = client.Call("Server.HandlePrepareMessage", prepareMsg, &promiseMessage)
			if err != nil {
				log.Printf("Error calling remote procedure: %v\n", err)
			}
		}
	}
	// s.processPendingTransactions()
}

func (s *Server) processPendingTransactions(repeat bool) {

	// fmt.Printf("Printing active status of server: ", s.ActiveStatus)
	for _, transaction := range s.PendingTxn {
		if s.Balances >= transaction.Amount {

			s.Balances -= transaction.Amount
			if s.Name == transaction.Sender {
				s.LocalTxn = append(s.LocalTxn, transaction)
			}

			log.Printf("Server %s: Processing pending transaction: %s sends %d to %s", s.Name, transaction.Sender, transaction.Amount, transaction.Receiver)

			log.Printf("New Balance for Server %s: %d", s.Name, s.Balances)
			s.txn_executed++
			// log.Printf("Current Local Transaction for Server %s: ", s.Name, s.LocalTxn)
		} else {
			if repeat {
				// s.initiatePaxos()
			}
			log.Printf("Server %s: Still insufficient funds for transaction: %s sends %d to %s", s.Name, transaction.Sender, transaction.Amount, transaction.Receiver)
		}
	}

	s.PendingTxn = []Transaction{}
}

func (s *Server) HandlePrepareMessage(prepareMsg *PrepareMessage, promiseMsg *PromiseMessage) error {

	// function handled by follower servers
	// log.Print(prepareMsg)
	// log.Print(s.ActiveStatus)
	// log.Print(s.Name)
	if !s.ActiveStatus[s.Name] {
		// log.Printf("REJECT; server %s is inactive", s.Name)
		// prepareMsg.BallotPair.BallotNumber = s.AcceptNum.BallotNumber
		// s.AcceptNum.BallotNumber = prepareMsg.BallotPair.BallotNumber
		s.BallotPair.BallotNumber = prepareMsg.BallotPair.BallotNumber
		// log.Printf("Accept Num of inactive server: ", s.AcceptNum.BallotNumber)
		// log.Printf("Ballot Num of inactive server: ", s.AcceptNum.BallotNumber)
		return nil
	}
	log.Print("Received Prepare Msg from Server: ", prepareMsg.Sender)
	// log.Print(len(s.DataStore), prepareMsg.LenDataStore)

	if len(s.DataStore) < prepareMsg.LenDataStore {
		log.Printf("Follower Sync Required \n")
		// follower sync
		s.FollowerSync(prepareMsg.BallotPair.ServerID)

	} else if len(s.DataStore) > prepareMsg.LenDataStore {
		log.Printf("Leader Sync Required \n")
		promiseMsg.Ack = false
		promiseMsg.LenDataStore = len(s.DataStore)
		promiseMsg.ServerId = s.ServerID

		client, err := rpc.Dial("tcp", fmt.Sprintf("localhost:800%d", prepareMsg.BallotPair.ServerID))
		if err != nil {
			return fmt.Errorf("could not connect to leader %s: %v", prepareMsg.Sender, err)
		}
		defer client.Close()

		var responseMsg ResponseMessage
		err = client.Call("Server.ReceivePromise", promiseMsg, &responseMsg)
		if err != nil {
			return fmt.Errorf("error sending promise message to leader %s: %v", prepareMsg.Sender, err)
		}

	}

	if prepareMsg.BallotPair.BallotNumber > s.BallotPair.BallotNumber {

		promiseMsg.Ack = true
		promiseMsg.LenDataStore = len(s.DataStore)
		promiseMsg.ServerId = s.ServerID
		promiseMsg.BallotPair = BallotPair{
			BallotNumber: prepareMsg.BallotPair.BallotNumber,
			ServerID:     prepareMsg.BallotPair.ServerID,
		}

		// s.AcceptNum = BallotPair{
		// 	BallotNumber: prepareMsg.BallotPair.BallotNumber,
		// 	ServerID:     prepareMsg.BallotPair.ServerID,
		// }

		promiseMsg.AcceptNum = s.AcceptNum // Send the highest accepted ballot number so far
		promiseMsg.AcceptVal = s.AcceptVal // Send the latest committed block of transactions
		promiseMsg.LocalTxn = s.LocalTxn   // Send local transactions

		s.BallotPair.BallotNumber = prepareMsg.BallotPair.BallotNumber // setting other server's ballot number equal to leader's ballot number

		// fmt.Printf("------s.AcceptNum.BallotNum: ", s.AcceptNum.BallotNumber)
		// fmt.Printf("------s.BallotPair: ", s.BallotPair.BallotNumber)
		// fmt.Print(prepareMsg)
		// fmt.Printf("Server %s: Received prepare message from Server %s. Sending promise with BallotPair: (%d, %d) and added local transaction: %v\n", s.Name, prepareMsg.Sender, prepareMsg.BallotPair.BallotNumber, prepareMsg.BallotPair.ServerID, s.LocalTxn)
		// fmt.Printf("Promise Message AcceptVal from Server %s: %+v\n", s.Name, promiseMsg.AcceptVal)

		client, err := rpc.Dial("tcp", fmt.Sprintf("localhost:800%d", prepareMsg.BallotPair.ServerID))
		if err != nil {
			return fmt.Errorf("could not connect to leader %s: %v", prepareMsg.Sender, err)
		}
		defer client.Close()

		var responseMsg ResponseMessage
		err = client.Call("Server.ReceivePromise", promiseMsg, &responseMsg)
		if err != nil {
			return fmt.Errorf("error sending promise message to leader %s: %v", prepareMsg.Sender, err)
		}
		// return nil

	}
	// else {
	// 	// If the prepare message has a lower ballot number, do not send a promise
	// 	promiseMsg.Ack = false
	// 	fmt.Printf("Server %s: Rejected prepare message from Server %s with lower ballot number.\n",
	// 		s.Name, prepareMsg.Sender)
	// }

	return nil
}

func (s *Server) ReceivePromise(promiseMsg *PromiseMessage, responseMsg *ResponseMessage) error {

	s.PromisesReceived++
	// log.Printf("MINOR BLOCK: %v", minorBlock)

	minorBlock = append(minorBlock, promiseMsg.LocalTxn...) // appending other servers transactions

	responseMsg.Acknowledged = true
	// majority := (s.ActiveCount / 2) + 1
	majority := 3
	// log.Printf("Promise Received: %d", s.PromisesReceived)
	if s.PromisesReceived == majority {
		// log.Print("Majority Promise Rec")

		time.Sleep(20 * time.Millisecond)
	}
	// log.Print(promiseMsg)
	if promiseMsg.LenDataStore > s.highest_len_datastore {
		s.highest_len_datastore = promiseMsg.LenDataStore
		s.Highest_Len_Server_ID = promiseMsg.ServerId
	}
	if s.PromisesReceived == (s.ActiveCount - 1) {

		minorBlock = append(minorBlock, s.LocalTxn...) // appending leader's own transaction
		// s.ExecutedTxn = append(s.ExecutedTxn, s.LocalTxn...) //adding leader's own txn to avoid calculations in balance twice

		// log.Printf("Current Local Transaction for Server %s: ", s.Name, s.LocalTxn)
		// log.Printf("Current Executed Transaction for Server %s: ", s.Name, s.ExecutedTxn)

		if len(s.DataStore) < s.highest_len_datastore {
			// log.Print("I AM UNDERGOING SYNCING from: ",s.Highest_Len_Server_ID)
			s.PromisesReceived = 0
			s.acceptedCount = 0
			log.Print(s.PromisesReceived)
			minorBlock = []Transaction{}
			s.FollowerSync(s.Highest_Len_Server_ID)
			return nil
		}
		if s.PromisesReceived+1 >= majority {
			// fmt.Printf("Server %s: Majority promises received. Proceeding to accept phase.\n", s.Name)
			// fmt.Printf("MINOR BLOCK: \n", minorBlock)
			acceptMsg := AcceptMessage{
				BallotPair: promiseMsg.BallotPair,
				MajorBlock: minorBlock,
			}
			log.Print("Majority Promises Received...Sending Accept Messages")

			s.LocalTxn = []Transaction{} // clearing leader's local logs after pushing to MajorBlock
			// promiseMsg.LocalTxn = []Transaction{} // clearing promise message local transactions of other servers
			s.PromisesReceived = 0
			s.AcceptNum = acceptMsg.BallotPair
			s.AcceptVal = acceptMsg.MajorBlock
			s.handleAcceptMessage(acceptMsg)
			minorBlock = []Transaction{}
		} else {
			log.Print("PAXOS FAILED: NOT SUFFICIENT PROMISES RECEIVED")
			s.PromisesReceived = 0
			log.Print(s.PromisesReceived)
			return nil
		}
	}

	return nil
}

func (s *Server) handleAcceptMessage(acceptMsg AcceptMessage) {

	for id := 1; id <= 5; id++ {
		if id != s.ServerID {

			addr := fmt.Sprintf("localhost:800%d", id)
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				fmt.Printf("Error connecting to server %d: %v\n", id, err)
				return
			}
			defer client.Close()

			var response string
			err = client.Call("Server.ReceiveAccept", &acceptMsg, &response)
			if err != nil {
				fmt.Printf("Error sending accept message to server %d: %v\n", id, err)
			} else {
				// fmt.Printf("Server %d sent accept message to server %d\n", s.ServerID, id)
			}
		}
	}

}

func (s *Server) ReceiveAccept(acceptMsg *AcceptMessage, response *string) error {

	if !s.ActiveStatus[s.Name] {
		// log.Printf("REJECT; server %s is inactive", s.Name)
		return nil
	}

	// fmt.Printf("Server %d: Received accept message: %+v\n", s.ServerID, *acceptMsg)
	s.AcceptNum = acceptMsg.BallotPair
	s.AcceptVal = acceptMsg.MajorBlock
	acceptedMsg := AcceptedMessage{
		BallotPair: acceptMsg.BallotPair,
		MajorBlock: acceptMsg.MajorBlock,
	}

	leaderAddr := fmt.Sprintf("localhost:800%d", acceptMsg.BallotPair.ServerID)
	client, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		log.Fatalf("Error connecting to leader: %v\n", err)
	}

	defer client.Close()

	var leaderResponse ResponseMessage
	err = client.Call("Server.ReceiveAccepted", &acceptedMsg, &leaderResponse)
	if err != nil {
		fmt.Printf("Error sending accepted message to leader: %v\n", err)
	} else {
		fmt.Printf("Server %d: Sent accepted message to leader\n", s.ServerID)
	}

	*response = "Accept message received and Accepted message sent"
	return nil
}

func (s *Server) ReceiveAccepted(acceptedMsg *AcceptedMessage, responseMsg *ResponseMessage) error {

	s.acceptMessagesLock.Lock()
	defer s.acceptMessagesLock.Unlock()

	// fmt.Printf("Leader Server %d: Received accepted message: %+v\n", s.ServerID, *acceptedMsg)

	s.acceptedCount++

	majority := 3

	if s.acceptedCount >= majority {
		time.Sleep(20 * time.Millisecond)
	}

	if s.acceptedCount == s.ActiveCount-1 {

		// fmt.Printf("Leader Server %d: Majority of accepted messages received. Sending commit message.\n", s.ServerID)

		for _, txn := range acceptedMsg.MajorBlock {
			if txn.Receiver == s.Name {
				s.Balances += txn.Amount
			}
		}
		s.LocalTxn = []Transaction{} // clearing leader's local logs after pushing to MajorBlock

		fmt.Printf("Leader Server %d: Updated balances after PAXOS: %d\n", s.ServerID, s.Balances)
		s.DataStore = append(s.DataStore, s.AcceptVal)
		s.highest_len_datastore = len(s.DataStore)
		s.Highest_Len_Server_ID = s.ServerID
		s.AcceptVal = nil
		s.AcceptNum = BallotPair{BallotNumber: 0, ServerID: s.ServerID}
		commitMsg := CommitMessage{
			FinalBlock: acceptedMsg.MajorBlock,
		}
		s.acceptedCount = 0
		s.sendCommitMessage(commitMsg)
	}

	responseMsg.Acknowledged = true
	return nil
}

func (s *Server) sendCommitMessage(commitMsg CommitMessage) {

	for id := 1; id <= 5; id++ {
		if id != s.ServerID {

			addr := fmt.Sprintf("localhost:800%d", id)
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				fmt.Printf("Error connecting to server %d: %v\n", id, err)
				return
			}
			defer client.Close()

			var response ResponseMessage
			err = client.Call("Server.ReceiveCommit", &commitMsg, &response)
			if err != nil {
				fmt.Printf("Error sending commit message to server %d: %v\n", id, err)
			} else {
				// fmt.Printf("Leader Server %d: Sent commit message to server %d\n", s.ServerID, id)
			}

		}
	}
	fmt.Printf("BALANCE: %d \n", s.Balances)

	// fmt.Printf("Pending Transaction: ", s.PendingTxn)
	// s.LocalTxn = append(s.LocalTxn, s.PendingTxn...)
	// fmt.Printf("Local Transaction of Leader: ", s.LocalTxn)

	s.processPendingTransactions(false)

}

func (s *Server) ReceiveCommit(commitMsg *CommitMessage, responseMsg *ResponseMessage) error {
	if !s.ActiveStatus[s.Name] {
		// log.Printf("REJECT; server %s is inactive", s.Name)
		// fmt.Printf("Server %d: Latest balance: %+v\n", s.ServerID, s.Balances)
		return nil
	}
	s.balanceLock.Lock()
	defer s.balanceLock.Unlock()
	s.LocalTxn = []Transaction{} // clearing leader's local logs after pushing to MajorBlock

	for _, txn := range s.AcceptVal {
		if txn.Receiver == s.Name {
			s.Balances += txn.Amount
		}
	}
	s.AcceptNum = BallotPair{BallotNumber: 0, ServerID: s.ServerID}
	s.DataStore = append(s.DataStore, s.AcceptVal)
	s.highest_len_datastore = len(s.DataStore)
	s.Highest_Len_Server_ID = s.ServerID
	s.AcceptVal = nil

	fmt.Printf("Server %d: Received commit message. Updated balances: %+v\n", s.ServerID, s.Balances)
	// fmt.Printf("Server %d: Latest balance: %+v\n", s.ServerID, s.Balances)

	responseMsg.Acknowledged = true
	return nil
}

func (s *Server) FollowerSync(catchup_server_id int) {

	request := FetchDataStoreRequest{ServerID: s.ServerID,
		LenDatastore: len(s.DataStore),
	}
	// Assuming s.LeaderAddress is the leader server address (host:port)
	leaderAddr := fmt.Sprintf("localhost:800%d", catchup_server_id)

	client, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		log.Printf("Server %d: Error connecting to leader: %v\n", s.ServerID, err)
		return
	}
	defer client.Close()
	var response FetchDataStoreResponse

	err = client.Call("Server.GetDataStore", request, &response)
	if err != nil {
		log.Printf("Server %d: Error during follower sync RPC call: %v\n", s.ServerID, err)
		return
	}

	for _, block := range response.PendingTransaction {
		for _, pend_txn := range block {
			if pend_txn.Receiver == s.Name {
				s.Balances += pend_txn.Amount

			}
			// log.Print(pend_txn)
		}
		s.DataStore = append(s.DataStore, block)
	}
	s.highest_len_datastore = len(s.DataStore)
	s.Highest_Len_Server_ID = s.ServerID
	log.Print("Updated Balance After Sync: ", s.Balances)
	s.processPendingTransactions(true)
	// log.Printf("Server %d: Follower sync completed. Updated DataStore: %+v\n", s.ServerID, s.DataStore)

}

func (s *Server) GetDataStore(request *FetchDataStoreRequest, response *FetchDataStoreResponse) error {

	// var response FetchDataStoreResponse

	for idx, major_block := range s.DataStore {
		if idx+1 > request.LenDatastore {
			response.PendingTransaction = append(response.PendingTransaction, major_block)
		}
	}
	return nil
}

func main() {

	// rand.Seed(time.Now().UnixNano())

	if len(os.Args) < 3 {
		fmt.Println("Usage: go run server.go <server_name> <port>")
		return
	}

	serverName := os.Args[1]
	port := os.Args[2]

	var serverID int
	switch serverName {
	case "S1":
		serverID = 1
	case "S2":
		serverID = 2
	case "S3":
		serverID = 3
	case "S4":
		serverID = 4
	case "S5":
		serverID = 5
	default:
		log.Fatalf("Invalid server name: %s. Must be one of S1, S2, S3, S4, or S5.", serverName)
		return
	}

	server := Server{
		Name:     serverName,
		Port:     port,
		ServerID: serverID,
		Balances: 100,
	}
	server.startServer()
}
