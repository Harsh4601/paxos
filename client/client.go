package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	// "time"
)

type Transaction struct {
	Sender   string
	Receiver string
	Amount   int
}

type ActiveStatus struct {
	ActiveServers map[string]bool
}

func callRPC(serverAddr string, transaction Transaction) {
	// Log the transaction details before making the RPC call
	// log.Printf("Sending transaction to server: %s, Transaction: Sender=%s, Receiver=%s, Amount=%d\n", serverAddr, transaction.Sender, transaction.Receiver, transaction.Amount)
	client, err := rpc.Dial("tcp", serverAddr)
	if err != nil {
		log.Fatalf("Error connecting to server: %v", err)
	}
	defer client.Close()

	var reply string
	err = client.Call("Server.HandleTransaction", transaction, &reply)
	if err != nil {
		log.Fatalf("Error calling remote procedure: %v", err)
	}
	// fmt.Printf("Transaction sent to %s, Server reply: %s\n", serverAddr, reply)
}

func sendActiveStatus(activeStatus ActiveStatus) {
	// Log the active status before sending it
	// log.Printf("Sending active status to all servers: %v\n", activeStatus.ActiveServers)
	// log.Print(activeStatus)

	for server := range activeStatus.ActiveServers {

		serverAddr := getServerAddress(strings.TrimSpace(server)) // server address based on the server name
		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			log.Fatalf("Error connecting to server: %v", err)
		}
		defer client.Close()

		var reply string
		err = client.Call("Server.HandleActiveStatus", activeStatus, &reply) 
		if err != nil {
			log.Fatalf("Error calling remote procedure: %v", err)
		}
		// fmt.Printf("Active status sent to %s, Server reply: %s\n", serverAddr, reply)
	}
}

func sendCheckBalance() {

	server_name := []string{"S1", "S2", "S3", "S4", "S5"}
	for _, server := range server_name {

		serverAddr := getServerAddress(server) //  server address based on the server name
		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			log.Fatalf("Error connecting to server: %v", err)
		}
		defer client.Close()

		var reply int
		err = client.Call("Server.CheckBalance", server, &reply) 
		if err != nil {
			log.Fatalf("Error calling remote procedure: %v", err)
		}
		fmt.Printf("Balance of Client %s: %d\n", server, reply)
	}
}

func sendCheckLog() {

	server_name := []string{"S1", "S2", "S3", "S4", "S5"}
	for _, server := range server_name {

		serverAddr := getServerAddress(server) //  server address based on the server name
		// log.Print(serverAddr)
		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			log.Fatalf("Error connecting to server: %v", err)
		}
		defer client.Close()

		var reply []Transaction
		err = client.Call("Server.CheckLog", server, &reply) 
		if err != nil {
			log.Fatalf("Error calling remote procedure: %v", err)
		}
		fmt.Printf("Log of Client %s: %v\n", server, reply)
	}
}

func sendPerformance() {

	server_name := []string{"S1", "S2", "S3", "S4", "S5"}
	for _, server := range server_name {

		serverAddr := getServerAddress(server) //  server address based on the server name
		// log.Print(serverAddr)
		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			log.Fatalf("Error connecting to server: %v", err)
		}
		defer client.Close()

		var reply []float64
		err = client.Call("Server.SendPerformance", server, &reply) 
		if err != nil {
			log.Fatalf("Error calling remote procedure: %v", err)
		}
		throughput := reply[0] / reply[1]

		fmt.Printf("Performance of Client %s:\n", server)
		fmt.Printf("Total Latency (in seconds): %f\n", reply[1])
		fmt.Printf("Total Transaction: %f\n", reply[0])
		fmt.Printf("Throughput: %f \n\n", throughput)

	}
}
func sendCheckDB() {

	server_name := []string{"S1", "S2", "S3", "S4", "S5"}
	for _, server := range server_name {

		serverAddr := getServerAddress(server) //  server address based on the server name
		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			log.Fatalf("Error connecting to server: %v", err)
		}
		defer client.Close()

		var reply [][]Transaction
		err = client.Call("Server.CheckDatastore", server, &reply) 
		if err != nil {
			log.Fatalf("Error calling remote procedure: %v", err)
		}
		fmt.Printf("Database of Client %s: %v\n", server, reply)
	}
}

func processTransactions(csvFile string) {
	file, err := os.Open(csvFile)
	if err != nil {
		log.Fatalf("Failed to open CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read CSV file: %v", err)
	}

	var currentSet int
	var currentTransactions []Transaction
	var activeServers []string

	allServers := []string{"S1", "S2", "S3", "S4", "S5"}

	for _, record := range records {
		if len(record) == 0 {
			continue
		}

		if record[2] != "" {
			if currentSet != 0 {
				fmt.Printf("Processing transactions for Set %d:\n", currentSet)

				activeServerMap := make(map[string]bool)

				for _, server := range allServers {
					activeServerMap[strings.TrimSpace(server)] = false
				}

				for _, server := range activeServers {
					activeServerMap[strings.TrimSpace(server)] = true
				}

				activeStatus := ActiveStatus{ActiveServers: activeServerMap}

				sendActiveStatus(activeStatus)

				for _, txn := range currentTransactions {

					serverAddr := getServerAddress(txn.Sender)
					callRPC(serverAddr, txn)
					// time.Sleep(2000 * time.Millisecond)
				}
				var response int

				for {
					fmt.Print("1. Continue to the next set? \n")
					fmt.Print("2. Print Balance \n")
					fmt.Print("3. Print Log \n")
					fmt.Print("4. Print Database \n")
					fmt.Print("5. Performance Metrics\n")
					fmt.Print("6. Exit\n")
					fmt.Scanln(&response)
					if response == 1 {
						break
					}
					switch response {
					case 1:
						break
					case 2:
						sendCheckBalance()
					case 3:
						sendCheckLog()
					case 4:
						sendCheckDB()
					case 5:
						sendPerformance()

					case 6:
						break

					}

				}
				if response == 6 {
					break
				}

			}

			// Reset for the next set
			currentSet, _ = strconv.Atoi(record[0])
			activeServers = parseActiveServers(strings.TrimSpace(record[2])) // Parsing active servers from the first line
			// log.Print(activeServers)
			currentTransactions = nil // Clearing the previous transactions
		}

		// Adding transaction for the current set
		if len(record) > 1 {
			transactionStr := record[1] 
			transaction := parseTransaction(transactionStr)
			currentTransactions = append(currentTransactions, transaction)
		}
	}


	if currentSet != 0 && len(currentTransactions) > 0 {
		fmt.Printf("Processing transactions for Set %d:\n", currentSet)

		activeServerMap := make(map[string]bool)

		for _, server := range allServers {
			activeServerMap[server] = false
		}

		for _, server := range activeServers {
			activeServerMap[server] = true
		}

		activeStatus := ActiveStatus{ActiveServers: activeServerMap}

		sendActiveStatus(activeStatus)

		for _, txn := range currentTransactions {

			serverAddr := getServerAddress(txn.Sender)
			callRPC(serverAddr, txn)
		}
	}
	var response int
	for {
		fmt.Print("1. Continue to the next set? \n")
		fmt.Print("2. Print Balance \n")
		fmt.Print("3. Print Log \n")
		fmt.Print("4. Print Database \n")
		fmt.Print("5. Print Performance\n")
		fmt.Print("6. Exit\n")
		fmt.Scanln(&response)
		if response == 1 {
			break
		}
		switch response {
		case 1:
			break
		case 2:
			sendCheckBalance()
		case 3:
			sendCheckLog()
		case 4:
			sendCheckDB()
		case 5:
			sendPerformance()

		case 6:
			break

		}
	}

}

func parseActiveServers(activeServersStr string) []string {
	activeServersStr = strings.Trim(activeServersStr, "[]")
	return strings.Split(activeServersStr, ",")
}

func parseTransaction(transStr string) Transaction {
	transStr = strings.Trim(transStr, "()")
	parts := strings.Split(transStr, ",")
	sender := strings.TrimSpace(strings.TrimSpace(parts[0]))
	receiver := strings.TrimSpace(strings.TrimSpace(parts[1]))
	amount, _ := strconv.Atoi(strings.TrimSpace(parts[2]))
	return Transaction{Sender: sender, Receiver: receiver, Amount: amount}
}

func getServerAddress(sender string) string {
	serverMap := map[string]string{
		"S1": "localhost:8001",
		"S2": "localhost:8002",
		"S3": "localhost:8003",
		"S4": "localhost:8004",
		"S5": "localhost:8005",
	}
	return serverMap[sender]
}

func main() {
	csvFile := "lab1_Test.csv"
	processTransactions(csvFile)
}
