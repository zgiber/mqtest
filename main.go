package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	mqtt "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	tm "github.com/buger/goterm"
)

var (
	hostName    = flag.String("host", "127.0.0.1", "MQTT Server's hostname or IPv4 address.")
	port        = flag.String("port", "1883", "MQTT connection's port.")
	qos         = flag.Int("qos", 0, "QoS setting for the test\n\t0 - Fire and forget\n\t1 - Deliver at least 1x\n\t2 - Deliver exactly 1x")
	retained    = flag.Bool("retained", false, "If retained is true, the last published value for a topic is stored on the MQTT server.")
	numPub      = flag.Int("pub", 1, "Number of concurrent publishers.")
	numSub      = flag.Int("sub", 1, "Number of concurrent subscribers.")
	numTop      = flag.Int("topics", 1, "Number of topics per publisher.")
	numMessages = flag.Int("messages", 10, "Number of messages to be sent out per publisher.")
	pubDelay    = flag.Int("delay", 100, "Delay in ms between messages for a single publisher.")
	subTimeout  = flag.Int("timeout", 5, "Timeout in seconds for the subscriber.")
	user        = flag.String("userName", "", "Username.")
	pass        = flag.String("password", "", "Password.")
	nograph     = flag.Bool("nograph", false, "Hide graph. Recommended on slow remote terminals.")

	topics  = []string{}
	clients = []string{}
	tLock   sync.RWMutex
	cLock   sync.RWMutex
)

func getTopics() []string {
	t := make([]string, len(topics))
	tLock.RLock()
	copy(t, topics)
	tLock.RUnlock()
	return t
}

func getClients() []string {
	c := make([]string, len(clients))
	cLock.RLock()
	copy(c, clients)
	cLock.RUnlock()
	return c
}

func newClientID() string {
	cLock.Lock()
	clientId := fmt.Sprintf("client%v", len(clients))
	clients = append(clients, clientId)
	cLock.Unlock()
	return clientId
}

func newTopic() string {
	tLock.Lock()
	topic := fmt.Sprintf("topic%v", len(topics))
	topics = append(topics, topic)
	tLock.Unlock()
	return topic
}

type client struct {
	client *mqtt.Client
}

func newClient() *client {
	options := mqtt.NewClientOptions()
	options.AddBroker(fmt.Sprintf("tcp://%s:%s", *hostName, *port))
	options.SetClientID(newClientID())

	if len(*pass) > 0 {
		options.SetPassword(*pass)
	}

	if len(*user) > 0 {
		options.SetUsername(*user)
	}

	// TODO: TLS/SSL

	return &client{
		client: mqtt.NewClient(options),
	}
}

func main() {
	var (
		errCh        = make(chan error, 16)
		receivedCh   = make(chan int, 1024)
		sentCh       = make(chan int, 1024)
		sent         = 0
		lastSent     = 0
		received     = 0
		lastReceived = 0
	)

	// read params (server address:port, qos settings, number of publishers, number of subscribers)
	flag.Parse()

	// generate topics in advance, so we can run the subscribers before starting to publish
	for i := 0; i < *numTop; i++ {
		newTopic()
	}

	//
	// subscribe to topics
	//

	// TODO: multiple subscribers.

	// discarding received messages
	messageHandler := func(client *mqtt.Client, m mqtt.Message) {
		if string(m.Payload()) == "hello world" {
			receivedCh <- 1
		}
	}

	// prepare filter from topics and qos
	filters := map[string]byte{}
	for _, topic := range getTopics() {
		fmt.Printf("Created topic %s with qos %v\n", topic, *qos)
		filters[topic] = byte(*qos)
	}

	// multisubscribers
	fmt.Println("Connecting subscribers...")

	for i := 0; i < *numSub; i++ {
		subscriber := newClient()
		if token := subscriber.client.Connect(); token.Wait() && token.Error() != nil {
			errCh <- token.Error()
		}

		defer subscriber.client.Disconnect(250)

		token := subscriber.client.SubscribeMultiple(filters, messageHandler)
		if token.Wait() && token.Error() != nil {
			errCh <- token.Error()
		}

	}

	//
	// Publish to topics
	//

	// timeout if no messages are received in the given time since the last publish.
	timeout := time.NewTimer(time.Duration(*subTimeout) * time.Second)
	resetSubTimeout := func() {
		timeout.Reset(time.Duration(*subTimeout) * time.Second)
	}

	go func() {
		for {
			select {
			case <-timeout.C:
				errCh <- fmt.Errorf("Subscriber timeout.. no more messages?")
			}
		}
	}()

	// publishers
	for i := 0; i < *numPub; i++ {
		go func() {
			c := newClient()
			if token := c.client.Connect(); token.Wait() && token.Error() != nil {
				errCh <- token.Error()
			}

			defer c.client.Disconnect(100)

			// publish (sequential per client)
			topics := getTopics()
			for k := 0; k < *numMessages; k++ {
				topic := topics[k%len(topics)]
				token := c.client.Publish(topic, byte(*qos), *retained, "hello world")
				if token.Wait() && token.Error() != nil {
					errCh <- token.Error()
				}
				sentCh <- 1
				resetSubTimeout() // reset timeout
				time.Sleep(time.Duration(*pubDelay) * time.Millisecond)
			}
		}()
	}

	// creating fancy output
	// start := time.Now()
	redraw := time.NewTicker(100 * time.Millisecond)
	if *nograph {
		redraw = time.NewTicker(1 * time.Second)
	}
	row := 0.0

	// table
	data := new(tm.DataTable)
	data.AddColumn("Time (sec)")
	data.AddColumn("Sent")
	data.AddColumn("Received")

	for {
		select {
		case <-redraw.C:
			if !*nograph {
				tm.Clear()
				tm.Printf("[Published : Received]: [%v : %v]\n\n", sent, received)
				row += 1.0
				data.AddRow(row/10.0, float64(sent), float64(received))
				tm.Print(tm.NewLineChart(tm.Width()-4, tm.Height()-6).Draw(data))
				tm.Flush()
			} else {
				if sent != lastSent || received != lastReceived {
					fmt.Printf("[Published : Received]: [%v : %v]\n", sent, received)
					lastSent, lastReceived = sent, received
				}
			}

		case e := <-errCh:
			fmt.Println(e.Error())
			return
		case s := <-sentCh:
			sent += s
		case r := <-receivedCh:
			received += r
		}
	}
}
