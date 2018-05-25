package replica

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	pb "github.com/zballs/goPBFT/types"
)

const (
	CHECKPOINT_PERIOD uint64 = 128
	CONSTANT_FACTOR   uint64 = 2
)

var Fmt = fmt.Sprintf

type Replica struct {
	net.Listener

	ID       uint64
	replicas map[uint64]string

	activeView bool
	view       uint64
	sequence   uint64

	requestChan chan *pb.Request
	replyChan   chan *pb.Reply
	errChan     chan error

	requests  map[string][]*pb.Request
	replies   map[string][]*pb.Reply
	lastReply *pb.Reply

	// prep    []*pb.Entry
	// prePrep []*pb.Entry

	pendingVC []*pb.Request

	executed    []uint64
	checkpoints []*pb.Checkpoint
}

// Basic operations
func main() {
	/*
		log.Print("Creat Replica")
		replyChan := make(chan *pb.Reply)
		requestChan := make(chan *pb.Request)
		pn := &Replica{
			ID:          1,
			replicas:    make(map[uint64]string),
			activeView:  true,
			view:        1,
			sequence:    0,
			requestChan: requestChan,
			replyChan:   replyChan,
			errChan:     make(chan error),
			requests:    make(map[string][]*pb.Request),
			replies:     make(map[string][]*pb.Reply),
			//doneChan:    make(chan string),
			lastReply: nil,
			pendingVC: make([]*pb.Request, 10),
			executed:  make([]uint64, 10),
		}
		pn.replicas[uint64(1)] = "127.0.0.1:8090"
		t := time.Now().String()
		reply := pb.ToReply(1, t, "127.0.0.1:8080", 1, &pb.Result{})
		pn.replies["127.0.0.1:8080"] = append(pn.replies["127.0.0.1:8080"], reply)
		time.Sleep(1 * time.Second)
		pn.checkpoints = []*pb.Checkpoint{pb.ToCheckpoint(0, []byte(""))}
		go creatClient("127.0.0.1:8080")
		//clientConnections(requestChan, "127.0.0.1:8080")
		go pn.acceptConnections("127.0.0.1:8090")
		//requestChan <- req
		time.Sleep(20 * time.Second)
	*/
	_, _, _ = CreateReplica(1, "127.0.0.1:8090,127.0.0.1:8091,127.0.0.1:8092,127.0.0.1:8093", "127.0.0.1:8090")
	//_, _, _ = createReplica(2, "127.0.0.1:8090,127.0.0.1:8091,127.0.0.1:8092,127.0.0.1:8093", "127.0.0.1:8091")
	//_, _, _ = createReplica(3, "127.0.0.1:8090,127.0.0.1:8091,127.0.0.1:8092,127.0.0.1:8093", "127.0.0.1:8092")
	//_, _, _ = createReplica(4, "127.0.0.1:8090,127.0.0.1:8091,127.0.0.1:8092,127.0.0.1:8093", "127.0.0.1:8093")
	//go creatClient("127.0.0.1:8080")
	for {

	}
	//time.Sleep(10 * time.Second)
}

func CreateReplica(id uint64, PeersURL string, addr string) (chan *pb.Reply, chan *pb.Request, bool) {
	log.Print("Creat Replica id: ", id)
	replyChan := make(chan *pb.Reply)
	requestChan := make(chan *pb.Request)
	pn := &Replica{
		ID:          id,
		replicas:    make(map[uint64]string),
		activeView:  true,
		view:        1,
		sequence:    0,
		requestChan: requestChan,
		replyChan:   replyChan,
		errChan:     make(chan error),
		requests:    make(map[string][]*pb.Request),
		replies:     make(map[string][]*pb.Reply),
		//doneChan:    make(chan string),
		lastReply: nil,
		pendingVC: make([]*pb.Request, 10),
		executed:  make([]uint64, 10),
	}
	peers := strings.Split(PeersURL, ",")
	for num, peer := range peers {
		pn.replicas[uint64(num)] = peer
	}
	/*
		t := time.Now().String()
		reply := pb.ToReply(1, t, "127.0.0.1:8080", 1, &pb.Result{})
		pn.replies["127.0.0.1:8080"] = append(pn.replies["127.0.0.1:8080"], reply)
	*/
	pn.checkpoints = []*pb.Checkpoint{pb.ToCheckpoint(0, []byte(""))}
	pn.acceptConnections(addr)
	return replyChan, requestChan, pn.isPrimary(pn.ID)
}

func (rep *Replica) primary() uint64 {
	return rep.view % uint64(len(rep.replicas)+1)
}

func (rep *Replica) newPrimary(view uint64) uint64 {
	return view % uint64(len(rep.replicas)+1)
}

func (rep *Replica) isPrimary(ID uint64) bool {
	return ID == rep.primary()
}

func (rep *Replica) oneThird(count int) bool {
	return count >= (len(rep.replicas)+1)/3
}

func (rep *Replica) overOneThird(count int) bool {
	return count > (len(rep.replicas)+1)/3
}

func (rep *Replica) twoThirds(count int) bool {
	return count >= 2*(len(rep.replicas))/3
}

func (rep *Replica) overTwoThirds(count int) bool {
	return count > 2*(len(rep.replicas))/3
}

func (rep *Replica) lowWaterMark() uint64 {
	return rep.lastStable().Sequence
}

func (rep *Replica) highWaterMark() uint64 {
	return rep.lowWaterMark() + CHECKPOINT_PERIOD*CONSTANT_FACTOR
}

func (rep *Replica) sequenceInRange(sequence uint64) bool {
	return sequence > rep.lowWaterMark() && sequence <= rep.highWaterMark()
}

func (rep *Replica) lastExecuted() uint64 {
	return rep.executed[len(rep.executed)-1]
}

func (rep *Replica) lastStable() *pb.Checkpoint {
	return rep.checkpoints[len(rep.checkpoints)-1]
}

func (rep *Replica) theLastReply() *pb.Reply {
	lastReply := rep.lastReply
	for _, replies := range rep.replies {
		reply := replies[len(replies)-1]
		if reply.Timestamp > lastReply.Timestamp {
			lastReply = reply
		}
	}
	return lastReply
}

func (rep *Replica) lastReplyToClient(client string) *pb.Reply {
	//log.Print(rep.replies[client][len(rep.replies[client])-1])
	if v, ok := rep.replies[client]; ok {
		return v[len(rep.replies[client])-1]
	} else {
		return nil
	}
	//return rep.replies[client][len(rep.replies[client])-1]
}

func (rep *Replica) stateDigest() []byte {
	return rep.theLastReply().Digest()
}

func (rep *Replica) isCheckpoint(sequence uint64) bool {
	return sequence%CHECKPOINT_PERIOD == 0
}

func (rep *Replica) addCheckpoint(checkpoint *pb.Checkpoint) {
	rep.checkpoints = append(rep.checkpoints, checkpoint)
}

/*
func clientConnections(requestChan chan *pb.Request, addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Panic("tcp connect error", "err", err)
	}
	log.Print("Client Start Listen to ", addr)
	go func() {
		op := new(pb.Operation)
		for i := 1; i < 6; i++ {
			op.Value = uint64(i)
			t := time.Now().String()
			log.Print("Creat a client request")
			req := pb.ToRequestClient(op, t, addr)
			log.Print("The client request is ", req)
			requestChan <- req
			time.Sleep(2 * time.Second)
		}
	}()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Panic(err)
			}
			rep := &pb.Reply{}
			err = pb.ReadMessage(conn, rep)
			if err != nil {
				log.Panic(err)
			}
			log.Print("After read, the Reply is ", rep)
		}
	}()
}
*/

func creatClient(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Panic("Tcp connect error")
	}
	log.Print("Client start listen on ", addr)
	clientWriteMessage(addr, "127.0.0.1:8090,127.0.0.1:8091,127.0.0.1:8092,127.0.0.1:8093")
	go clientAcceptMessage(ln)
}

func clientAcceptMessage(ln net.Listener) {
	log.Print("Client start get message")
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Panic(err)
		}
		rep := &pb.Reply{}
		err = pb.ReadMessage(conn, rep)
		if err != nil {
			log.Panic(err)
		}
		log.Print("After read, the Reply is ", rep)
	}
}

func clientWriteMessage(clientaddr string, peeraddr string) {
	op := new(pb.Operation)
	op.Value = 10
	t := time.Now().String()
	log.Print("Creat a client request")
	req := pb.ToRequestClient(op, t, clientaddr)
	peers := strings.Split(peeraddr, ",")
	for _, peer := range peers {
		log.Print("Client Start to write message to ", peer)
		pb.WriteMessage(peer, req)
	}
}

func (rep *Replica) acceptConnections(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Panic("tcp connect error", "err", err)
	}
	log.Print("Replica ", rep.ID, " Start Listen to ", addr)
	go rep.sendRoutine()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Panic(err)
			}
			req := &pb.Request{}
			err = pb.ReadMessage(conn, req)
			if err != nil {
				log.Panic(err)
			}
			//log.Print("Replica ", rep.ID, " After read, the Request is ", req)
			rep.handleRequest(req)
		}
	}()
}

// Sends

func (rep *Replica) multicast(REQ *pb.Request) error {
	for _, replica := range rep.replicas {
		//log.Print("Replica ", rep.ID, " Start Write Request to ", replica)
		err := pb.WriteMessage(replica, REQ)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rep *Replica) sendRoutine() {
	for {
		select {
		case REQ := <-rep.requestChan:
			switch REQ.Value.(type) {
			case *pb.Request_Ack:
				view := REQ.GetAck().View
				primaryID := rep.newPrimary(view)
				primary := rep.replicas[primaryID]
				err := pb.WriteMessage(primary, REQ)
				if err != nil {
					go func() {
						rep.errChan <- err
					}()
				}
			default:
				err := rep.multicast(REQ)
				if err != nil {
					go func() {
						rep.errChan <- err
					}()
				}
			}
		case reply := <-rep.replyChan:
			client := reply.Client
			err := pb.WriteMessage(client, reply)
			if err != nil {
				go func() {
					rep.errChan <- err
				}()
			}
		}
	}
}

func (rep *Replica) allRequests() []*pb.Request {
	var requests []*pb.Request
	for _, reqs := range rep.requests {
		requests = append(requests, reqs...)
	}
	return requests
}

// Log
// !hasRequest before append?

func (rep *Replica) logRequest(REQ *pb.Request) {
	switch REQ.Value.(type) {
	case *pb.Request_Client:
		rep.requests["client"] = append(rep.requests["client"], REQ)
	case *pb.Request_Preprepare:
		rep.requests["pre-prepare"] = append(rep.requests["pre-prepare"], REQ)
	case *pb.Request_Prepare:
		rep.requests["prepare"] = append(rep.requests["prepare"], REQ)
	case *pb.Request_Commit:
		rep.requests["commit"] = append(rep.requests["commit"], REQ)
	case *pb.Request_Checkpoint:
		rep.requests["checkpoint"] = append(rep.requests["checkpoint"], REQ)
	case *pb.Request_Viewchange:
		rep.requests["view-change"] = append(rep.requests["view-change"], REQ)
	case *pb.Request_Ack:
		rep.requests["ack"] = append(rep.requests["ack"], REQ)
	case *pb.Request_Newview:
		rep.requests["new-view"] = append(rep.requests["new-view"], REQ)
	default:
		log.Printf("Replica %d tried logging unrecognized request type\n", rep.ID)
	}
}

func (rep *Replica) logPendingVC(REQ *pb.Request) error {
	switch REQ.Value.(type) {
	case *pb.Request_Viewchange:
		rep.pendingVC = append(rep.pendingVC, REQ)
		return nil
	default:
		return errors.New("Request is wrong type")
	}
}

func (rep *Replica) logReply(client string, reply *pb.Reply) bool {
	lastReplyToClient := rep.lastReplyToClient(client)
	if lastReplyToClient == nil || lastReplyToClient.Timestamp < reply.Timestamp {
		rep.replies[client] = append(rep.replies[client], reply)
		return true
	}
	return false
}

/*
func (rep *Replica) logPrep(prep *pb.RequestPrepare)
	rep.prep = append(rep.prep, prep)
}

func (rep *Replica) logPrePrep(prePrep *pb.RequestPrePrepare) {
	rep.prePrep = append(rep.prePrep, prePrep)
}
*/

// Has requests
// hasRequest用于判断是否该Replica的log中是否有例如Preprepare Prepare等Request
// 且这个Request要与输入REQ相同(相同view 相同Seq digest相同信息)
func (rep *Replica) hasRequest(REQ *pb.Request) bool {

	switch REQ.Value.(type) {
	case *pb.Request_Preprepare:
		return rep.hasRequestPreprepare(REQ)
	case *pb.Request_Prepare:
		return rep.hasRequestPrepare(REQ)
	case *pb.Request_Commit:
		return rep.hasRequestCommit(REQ)
	case *pb.Request_Viewchange:
		return rep.hasRequestViewChange(REQ)
	case *pb.Request_Ack:
		return rep.hasRequestAck(REQ)
	case *pb.Request_Newview:
		return rep.hasRequestNewView(REQ)
	default:
		return false
	}
}

func (rep *Replica) hasRequestPreprepare(REQ *pb.Request) bool {
	view := REQ.GetPreprepare().View
	sequence := REQ.GetPreprepare().Sequence
	digest := REQ.GetPreprepare().Digest
	for _, req := range rep.requests["pre-prepare"] {
		v := req.GetPreprepare().View
		s := req.GetPreprepare().Sequence
		d := req.GetPreprepare().Digest
		if v == view && s == sequence && pb.EQ(d, digest) {
			return true
		}
	}
	return false
}

func (rep *Replica) hasRequestPrepare(REQ *pb.Request) bool {
	view := REQ.GetPrepare().View
	sequence := REQ.GetPrepare().Sequence
	digest := REQ.GetPrepare().Digest
	replica := REQ.GetPrepare().Replica
	for _, req := range rep.requests["prepare"] {
		v := req.GetPrepare().View
		s := req.GetPrepare().Sequence
		d := req.GetPrepare().Digest
		r := req.GetPrepare().Replica
		if v == view && s == sequence && pb.EQ(d, digest) && r == replica {
			return true
		}
	}
	return false
}

func (rep *Replica) hasRequestCommit(REQ *pb.Request) bool {
	view := REQ.GetCommit().View
	sequence := REQ.GetCommit().Sequence
	replica := REQ.GetCommit().Replica
	for _, req := range rep.requests["commit"] {
		v := req.GetCommit().View
		s := req.GetCommit().Sequence
		r := req.GetCommit().Replica
		if v == view && s == sequence && r == replica {
			return true
		}
	}
	return false
}

func (rep *Replica) hasRequestViewChange(REQ *pb.Request) bool {
	view := REQ.GetViewchange().View
	replica := REQ.GetViewchange().Replica
	for _, req := range rep.requests["view-change"] {
		v := req.GetViewchange().View
		r := req.GetViewchange().Replica
		if v == view && r == replica {
			return true
		}
	}
	return false
}

func (rep *Replica) hasRequestAck(REQ *pb.Request) bool {
	view := REQ.GetAck().View
	replica := REQ.GetAck().Replica
	viewchanger := REQ.GetAck().Viewchanger
	for _, req := range rep.requests["ack"] {
		v := req.GetAck().View
		r := req.GetAck().Replica
		vc := req.GetAck().Viewchanger
		if v == view && r == replica && vc == viewchanger {
			return true
		}
	}
	return false
}

func (rep *Replica) hasRequestNewView(REQ *pb.Request) bool {
	view := REQ.GetNewview().View
	for _, req := range rep.requests["new-view"] {
		v := req.GetNewview().View
		if v == view {
			return true
		}
	}
	return false
}

/*
func (rep *Replica) clearEntries(sequence uint64) {
	go rep.clearPrepared(sequence)
	go rep.clearPreprepared(sequence)
}

func (rep *Replica) clearPrepared(sequence uint64) {
	prepared := rep.prepared
	for idx, entry := range rep.prepared {
		s := entry.sequence
		if s <= sequence {
			prepared = append(prepared[:idx], prepared[idx+1:]...)
		}
	}
	rep.prepared = prepared
}

func (rep *Replica) clearPreprepared(sequence uint64) {
	prePrepared := rep.prePrepared
	for idx, entry := range rep.prePrepared {
		s := entry.sequence
		if s <= sequence {
			prePrepared = append(prePrepared[:idx], prePrepared[idx+1:]...)
		}
	}
	rep.prePrepared = prePrepared
}
*/

// Clear requests

func (rep *Replica) clearRequestsBySeq(sequence uint64) {
	rep.clearRequestClients()
	rep.clearRequestPrepreparesBySeq(sequence)
	rep.clearRequestPreparesBySeq(sequence)
	rep.clearRequestCommitsBySeq(sequence)
	rep.clearRequestCheckpointsBySeq(sequence)
}

func (rep *Replica) clearRequestClients() {
	clientReqs := rep.requests["client"]
	lastTimestamp := rep.theLastReply().Timestamp
	for idx, req := range rep.requests["client"] {
		timestamp := req.GetClient().Timestamp
		if lastTimestamp >= timestamp {
			clientReqs = append(clientReqs[:idx], clientReqs[idx+1:]...)
		}
	}
	rep.requests["client"] = clientReqs
}

func (rep *Replica) clearRequestPrepreparesBySeq(sequence uint64) {
	prePrepares := rep.requests["pre-prepare"]
	for idx, req := range rep.requests["pre-prepare"] {
		s := req.GetPreprepare().Sequence
		if s <= sequence {
			prePrepares = append(prePrepares[:idx], prePrepares[idx+1:]...)
		}
	}
	rep.requests["pre-prepare"] = prePrepares
}

func (rep *Replica) clearRequestPreparesBySeq(sequence uint64) {
	prepares := rep.requests["prepare"]
	for idx, req := range rep.requests["prepare"] {
		s := req.GetPrepare().Sequence
		if s <= sequence {
			prepares = append(prepares[:idx], prepares[idx+1:]...)
		}
	}
	rep.requests["prepare"] = prepares
}

func (rep *Replica) clearRequestCommitsBySeq(sequence uint64) {
	commits := rep.requests["commit"]
	for idx, req := range rep.requests["commit"] {
		s := req.GetCommit().Sequence
		if s <= sequence {
			commits = append(commits[:idx], commits[idx+1:]...)
		}
	}
	rep.requests["commit"] = commits
}

func (rep *Replica) clearRequestCheckpointsBySeq(sequence uint64) {
	checkpoints := rep.requests["checkpoint"]
	for idx, req := range rep.requests["checkpoint"] {
		s := req.GetCheckpoint().Sequence
		if s <= sequence {
			checkpoints = append(checkpoints[:idx], checkpoints[idx+1:]...)
		}
	}
	rep.requests["checkpoint"] = checkpoints
}

func (rep *Replica) clearRequestsByView(view uint64) {
	rep.clearRequestPrepreparesByView(view)
	rep.clearRequestPreparesByView(view)
	rep.clearRequestCommitsByView(view)
	//add others
}

func (rep *Replica) clearRequestPrepreparesByView(view uint64) {
	prePrepares := rep.requests["pre-prepare"]
	for idx, req := range rep.requests["pre-prepare"] {
		v := req.GetPreprepare().View
		if v < view {
			prePrepares = append(prePrepares[:idx], prePrepares[idx+1:]...)
		}
	}
	rep.requests["pre-prepare"] = prePrepares
}

func (rep *Replica) clearRequestPreparesByView(view uint64) {
	prepares := rep.requests["prepare"]
	for idx, req := range rep.requests["prepare"] {
		v := req.GetPrepare().View
		if v < view {
			prepares = append(prepares[:idx], prepares[idx+1:]...)
		}
	}
	rep.requests["prepare"] = prepares
}

func (rep *Replica) clearRequestCommitsByView(view uint64) {
	commits := rep.requests["commit"]
	for idx, req := range rep.requests["commit"] {
		v := req.GetCommit().View
		if v < view {
			commits = append(commits[:idx], commits[idx+1:]...)
		}
	}
	rep.requests["commit"] = commits
}

// Handle requests

func (rep *Replica) handleRequest(REQ *pb.Request) {

	switch REQ.Value.(type) {
	case *pb.Request_Client:

		rep.handleRequestClient(REQ)

	case *pb.Request_Preprepare:

		rep.handleRequestPreprepare(REQ)

	case *pb.Request_Prepare:

		rep.handleRequestPrepare(REQ)

	case *pb.Request_Commit:

		rep.handleRequestCommit(REQ)

	case *pb.Request_Checkpoint:

		rep.handleRequestCheckpoint(REQ)

	case *pb.Request_Viewchange:

		rep.handleRequestViewChange(REQ)

	case *pb.Request_Ack:

		rep.handleRequestAck(REQ)

	default:
		fmt.Printf("Replica %d received unrecognized request type\n", rep.ID)
	}
}

func (rep *Replica) handleRequestClient(REQ *pb.Request) {
	log.Print("Replica ", rep.ID, " Handling Client Request")
	//log.Print(rep.requests)
	rep.sequence++

	client := REQ.GetClient().Client
	//log.Print("The client is ", client)
	timestamp := REQ.GetClient().Timestamp
	//log.Print("The timestamp is ", timestamp)

	lastReplyToClient := rep.lastReplyToClient(client)
	if lastReplyToClient != nil {
		if lastReplyToClient.Timestamp == timestamp {
			reply := pb.ToReply(rep.view, timestamp, client, rep.ID, lastReplyToClient.Result)
			rep.logReply(client, reply)
			go func() {
				rep.replyChan <- reply
			}()
			return
		}
	}

	rep.logRequest(REQ)

	//log.Print(rep.requests)

	if !rep.isPrimary(rep.ID) {
		log.Print("Repilca ", rep.ID, " is not Primary node")
		return
	}
	log.Print("Primary ", rep.ID, " Start Preprepare")
	req := pb.ToRequestPreprepare(rep.view, rep.sequence, REQ.Digest(), rep.ID)
	rep.logRequest(req)

	// prePrep := req.GetPreprepare()
	// rep.logPrePrep(prePrep)

	go func() {
		rep.requestChan <- req
	}()

	/*
		err := rep.multicast(prePrepare)
		if err != nil {
			return err
		}
	*/
}

func (rep *Replica) handleRequestPreprepare(REQ *pb.Request) {

	replica := REQ.GetPreprepare().Replica
	log.Print("Replica ", rep.ID, " Handling ", replica, "'s Preprepare Request")

	if !rep.isPrimary(replica) {
		log.Print("Preprepare not from primary node")
		return
	}

	view := REQ.GetPreprepare().View

	if rep.view != view {
		log.Print("view change")
		return
	}

	sequence := REQ.GetPreprepare().Sequence
	if !rep.sequenceInRange(sequence) {
		log.Print("sequence not in range")
		return
	}

	digest := REQ.GetPreprepare().Digest

	accept := true
	for _, req := range rep.requests["pre-prepare"] {
		v := req.GetPreprepare().View
		s := req.GetPreprepare().Sequence
		d := req.GetPreprepare().Digest

		if v == view && s == sequence && !pb.EQ(d, digest) {
			accept = false
			break
		}
	}

	if !accept {
		return
	}

	//log.Print(rep.requests)

	rep.logRequest(REQ)

	//log.Print(rep.requests)
	/*
		if rep.isPrimary() {
			return nil
		}
	*/
	log.Print(rep.ID, " Start to prepare")
	req := pb.ToRequestPrepare(view, sequence, digest, rep.ID)

	if rep.hasRequest(req) {
		return
	}

	rep.logRequest(req)

	go func() {
		rep.requestChan <- req
	}()

	// prePrep := REQ.GetPreprepare()
	// rep.logPrePrep(prePrep)
}

func (rep *Replica) handleRequestPrepare(REQ *pb.Request) {
	replica := REQ.GetPrepare().Replica
	log.Print("Replica ", rep.ID, " Handling ", replica, " Prepare Request")

	if rep.isPrimary(replica) {
		log.Print("Primary prepare message no need to handle")
		return
	}

	view := REQ.GetPrepare().View

	if rep.view != view {
		log.Print("view change")
		return
	}

	sequence := REQ.GetPrepare().Sequence

	if !rep.sequenceInRange(sequence) {
		log.Print("Prepare sequence not in range")
		return
	}
	digest := REQ.GetPrepare().Digest

	rep.logRequest(REQ)

	prePrepared := make(chan bool, 1)
	twoThirds := make(chan bool, 1)
	var pPed bool
	var tTs bool
	go func() {
		prePrepared <- rep.hasRequest(REQ)
	}()
	go func() {
		count := 0
		for _, req := range rep.requests["prepare"] {
			v := req.GetPrepare().View
			s := req.GetPrepare().Sequence
			d := req.GetPrepare().Digest
			r := req.GetPrepare().Replica
			if v != view || s != sequence || !pb.EQ(d, digest) {
				continue
			}
			if r == replica {
				log.Printf("Replica %d sent multiple prepare requests\n", replica)
				continue
			}
			count++
			if rep.twoThirds(count) {
				//log.Print(count, " >= 3")
				twoThirds <- true
				return
			}
		}
		twoThirds <- false
	}()
	/*
		if !<-twoThirds {
			return
		}
	*/
	pPed = <-prePrepared
	tTs = <-twoThirds
	if !tTs || !pPed {
		if !pPed {
			log.Print(rep.ID, " Not Preprepared")
		}
		if !tTs {
			log.Print("Count Not over 2/3")
		}
		return
	}
	//log.Print("Run here")

	log.Print(rep.ID, " Start to commit")
	req := pb.ToRequestCommit(view, sequence, rep.ID)
	if rep.hasRequest(req) {
		return
	}

	rep.logRequest(req)
	go func() {
		rep.requestChan <- req
	}()
}

func (rep *Replica) handleRequestCommit(REQ *pb.Request) {
	log.Print("Replica ", rep.ID, " Handling Commit Request")

	view := REQ.GetCommit().View

	if rep.view != view {
		return
	}

	sequence := REQ.GetCommit().Sequence

	if !rep.sequenceInRange(sequence) {
		return
	}

	replica := REQ.GetCommit().Replica
	if replica == rep.ID {
		return
	}
	rep.logRequest(REQ)
	//log.Print(rep.requests)
	//prePared := make(chan bool, 1)
	twoThirds := make(chan bool, 1)
	/*
		go func() {
			prePared <- rep.hasRequest(REQ)
		}()
	*/
	go func() {
		count := 0
		for _, req := range rep.requests["commit"] {
			v := req.GetCommit().View
			s := req.GetCommit().Sequence
			rr := req.GetCommit().Replica
			if v != view || s != sequence {
				continue
			}
			if rr == replica {
				//log.Printf("Replica %d sent multiple commit requests\n", replica)
				continue
			}
			count++
			//log.Print(count)
			if rep.overTwoThirds(count) {
				//log.Print(rep.requests)
				twoThirds <- true
				return
			}
		}
		twoThirds <- false
	}()
	if !<-twoThirds {
		return
	}
	/*
		if !<-twoThirds || !<-prePared {
			if !<-prePared {
				log.Print("Not Prepared")
			}
			return
		}
	*/
	var digest []byte
	digests := make(map[string]struct{})
	for _, req := range rep.requests["pre-prepare"] {
		v := req.GetPreprepare().View
		s := req.GetPreprepare().Sequence
		if v == view && s <= sequence {
			digests[string(req.GetPreprepare().Digest)] = struct{}{}
			if s == sequence {
				digest = req.GetPreprepare().Digest
				break
			}
		}
	}
	for _, req := range rep.requests["client"] {
		d := req.Digest()
		if _, exists := digests[string(d)]; !exists {
			continue
		}
		if !pb.EQ(d, digest) {
			// Unexecuted message op with
			// lower sequence number...
			// log.Print("Unequal")
			continue
		}
		//op := req.GetClient().Op
		timestamp := req.GetClient().Timestamp
		client := req.GetClient().Client
		result := &pb.Result{"OK"}

		rep.executed = append(rep.executed, sequence)
		reply := pb.ToReply(view, timestamp, client, rep.ID, result)
		//log.Print(rep.ID)
		isRepeat := rep.logReply(client, reply)
		if !isRepeat {
			return
		}
		rep.lastReply = reply

		go func() {
			log.Print("Start write reply")
			rep.replyChan <- reply
		}()

		if !rep.isCheckpoint(sequence) {
			log.Print("Not in Checkpoint")
			return
		}
		stateDigest := rep.stateDigest()
		req := pb.ToRequestCheckpoint(sequence, stateDigest, rep.ID)
		rep.logRequest(req)
		checkpoint := pb.ToCheckpoint(sequence, stateDigest)
		rep.addCheckpoint(checkpoint)
		go func() {
			rep.requestChan <- req
		}()
		return
	}
}

func (rep *Replica) handleRequestCheckpoint(REQ *pb.Request) {

	sequence := REQ.GetCheckpoint().Sequence

	if !rep.sequenceInRange(sequence) {
		return
	}

	digest := REQ.GetCheckpoint().Digest
	replica := REQ.GetCheckpoint().Replica

	count := 0
	for _, req := range rep.requests["checkpoint"] {
		s := req.GetCheckpoint().Sequence
		d := req.GetCheckpoint().Digest
		r := req.GetCheckpoint().Replica
		if s != sequence || !pb.EQ(d, digest) {
			continue
		}
		if r == replica {
			fmt.Printf("Replica %d sent multiple checkpoint requests\n", replica)
			continue
		}
		count++
		if !rep.overTwoThirds(count) {
			continue
		}
		// rep.clearEntries(sequence)
		rep.clearRequestsBySeq(sequence)
		return
	}
	return
}

func (rep *Replica) handleRequestViewChange(REQ *pb.Request) {

	view := REQ.GetViewchange().View

	if view < rep.view {
		return
	}

	reqViewChange := REQ.GetViewchange()

	for _, prep := range reqViewChange.GetPreps() {
		v := prep.View
		s := prep.Sequence
		if v >= view || !rep.sequenceInRange(s) {
			return
		}
	}

	for _, prePrep := range reqViewChange.GetPrepreps() {
		v := prePrep.View
		s := prePrep.Sequence
		if v >= view || !rep.sequenceInRange(s) {
			return
		}
	}

	for _, checkpoint := range reqViewChange.GetCheckpoints() {
		s := checkpoint.Sequence
		if !rep.sequenceInRange(s) {
			return
		}
	}

	if rep.hasRequest(REQ) {
		return
	}

	rep.logRequest(REQ)

	viewchanger := reqViewChange.Replica

	req := pb.ToRequestAck(
		view,
		rep.ID,
		viewchanger,
		REQ.Digest())

	go func() {
		rep.requestChan <- req
	}()
}

func (rep *Replica) handleRequestAck(REQ *pb.Request) {

	view := REQ.GetAck().View
	primaryID := rep.newPrimary(view)

	if rep.ID != primaryID {
		return
	}

	if rep.hasRequest(REQ) {
		return
	}

	rep.logRequest(REQ)

	replica := REQ.GetAck().Replica
	viewchanger := REQ.GetAck().Viewchanger
	digest := REQ.GetAck().Digest

	reqViewChange := make(chan *pb.Request, 1)
	twoThirds := make(chan bool, 1)

	go func() {
		for _, req := range rep.requests["view-change"] {
			v := req.GetViewchange().View
			vc := req.GetViewchange().Replica
			if v == view && vc == viewchanger {
				reqViewChange <- req
			}
		}
		reqViewChange <- nil
	}()

	go func() {
		count := 0
		for _, req := range rep.requests["ack"] {
			v := req.GetAck().View
			r := req.GetAck().Replica
			vc := req.GetAck().Viewchanger
			d := req.GetAck().Digest
			if v != view || vc != viewchanger || !pb.EQ(d, digest) {
				continue
			}
			if r == replica {
				fmt.Printf("Replica %d sent multiple ack requests\n", replica)
				continue
			}
			count++
			if rep.twoThirds(count) {
				twoThirds <- true
				return
			}
		}
		twoThirds <- false
	}()

	req := <-reqViewChange

	if req == nil || !<-twoThirds {
		return
	}

	rep.logPendingVC(req)

	// When to send new view?
	rep.requestNewView(view)
}

func (rep *Replica) handleRequestNewView(REQ *pb.Request) {

	view := REQ.GetNewview().View

	if view == 0 || view < rep.view {
		return
	}

	replica := REQ.GetNewview().Replica
	primary := rep.newPrimary(view)

	if replica != primary {
		return
	}

	if rep.hasRequest(REQ) {
		return
	}

	rep.logRequest(REQ)

	rep.processNewView(REQ)
}

func (rep *Replica) correctViewChanges(viewChanges []*pb.ViewChange) (requests []*pb.Request) {

	// Returns requests if correct, else returns nil

	valid := false
	for _, vc := range viewChanges {
		for _, req := range rep.requests["view-change"] {
			d := req.Digest()
			if !pb.EQ(d, vc.Digest) {
				continue
			}
			requests = append(requests, req)
			v := req.GetViewchange().View
			// VIEW or rep.view??
			if v == rep.view {
				valid = true
				break
			}
		}
		if !valid {
			return nil
		}
	}

	if rep.isPrimary(rep.ID) {
		reps := make(map[uint64]int)
		valid = false
		for _, req := range rep.requests["ack"] {
			reqAck := req.GetAck()
			reps[reqAck.Replica]++
			if rep.twoThirds(reps[reqAck.Replica]) { //-2
				valid = true
				break
			}
		}
		if !valid {
			return nil
		}
	}

	return
}

func (rep *Replica) correctSummaries(requests []*pb.Request, summaries []*pb.Summary) (correct bool) {

	// Verify SUMMARIES

	var start uint64
	var digest []byte
	digests := make(map[uint64][]byte)

	for _, summary := range summaries {
		s := summary.Sequence
		d := summary.Digest
		if _d, ok := digests[s]; ok && !pb.EQ(_d, d) {
			return
		} else if !ok {
			digests[s] = d
		}
		if s < start || start == uint64(0) {
			start = s
			digest = d
		}
	}

	var A1 []*pb.Request
	var A2 []*pb.Request

	valid := false
	for _, req := range requests {
		reqViewChange := req.GetViewchange()
		s := reqViewChange.Sequence
		if s <= start {
			A1 = append(A1, req)
		}
		checkpoints := reqViewChange.GetCheckpoints()
		for _, checkpoint := range checkpoints {
			if checkpoint.Sequence == start && pb.EQ(checkpoint.Digest, digest) {
				A2 = append(A2, req)
				break
			}
		}
		if rep.twoThirds(len(A1)) && rep.oneThird(len(A2)) {
			valid = true
			break
		}
	}

	if !valid {
		return
	}

	end := start + CHECKPOINT_PERIOD*CONSTANT_FACTOR

	for seq := start; seq <= end; seq++ {

		valid = false

		for _, summary := range summaries {

			if summary.Sequence != seq {
				continue
			}

			if summary.Digest != nil {

				var view uint64

				for _, req := range requests {
					reqViewChange := req.GetViewchange()
					preps := reqViewChange.GetPreps()
					for _, prep := range preps {
						s := prep.Sequence
						d := prep.Digest
						if s != summary.Sequence || !pb.EQ(d, summary.Digest) {
							continue
						}
						v := prep.View
						if v > view {
							view = v
						}
					}
				}

				verifiedA1 := make(chan bool, 1)

				// Verify A1
				go func() {

					var A1 []*pb.Request

				FOR_LOOP:
					for _, req := range requests {
						reqViewChange := req.GetViewchange()
						s := reqViewChange.Sequence
						if s >= summary.Sequence {
							continue
						}
						preps := reqViewChange.GetPreps()
						for _, prep := range preps {
							s = prep.Sequence
							if s != summary.Sequence {
								continue
							}
							d := prep.Digest
							v := prep.View
							if v > view || (v == view && !pb.EQ(d, summary.Digest)) {
								continue FOR_LOOP
							}
						}
						A1 = append(A1, req)
						if rep.twoThirds(len(A1)) {
							verifiedA1 <- true
							return
						}
					}
					verifiedA1 <- false
				}()

				verifiedA2 := make(chan bool, 1)

				// Verify A2
				go func() {

					var A2 []*pb.Request

					for _, req := range requests {
						reqViewChange := req.GetViewchange()
						prePreps := reqViewChange.GetPrepreps()
						for _, prePrep := range prePreps {
							s := prePrep.Sequence
							d := prePrep.Digest
							v := prePrep.View
							if s == summary.Sequence && pb.EQ(d, summary.Digest) && v >= view {
								A2 = append(A2, req)
								break
							}
						}
						if rep.oneThird(len(A2)) {
							verifiedA2 <- true
							return
						}
					}
					verifiedA2 <- false
				}()

				if !<-verifiedA1 || !<-verifiedA2 {
					continue
				}

				valid = true
				break

			} else {

				var A1 []*pb.Request

			FOR_LOOP:

				for _, req := range requests {

					reqViewChange := req.GetViewchange()

					s := reqViewChange.Sequence

					if s >= summary.Sequence {
						continue
					}

					preps := reqViewChange.GetPreps()
					for _, prep := range preps {
						if prep.Sequence == summary.Sequence {
							continue FOR_LOOP
						}
					}

					A1 = append(A1, req)
					if rep.twoThirds(len(A1)) {
						valid = true
						break
					}
				}
				if valid {
					break
				}
			}
		}
		if !valid {
			return
		}
	}

	return true
}

func (rep *Replica) processNewView(REQ *pb.Request) (success bool) {

	if rep.activeView {
		return
	}

	reqNewView := REQ.GetNewview()

	viewChanges := reqNewView.GetViewchanges()
	requests := rep.correctViewChanges(viewChanges)

	if requests == nil {
		return
	}

	summaries := reqNewView.GetSummaries()
	correct := rep.correctSummaries(requests, summaries)

	if !correct {
		return
	}

	var h uint64
	for _, checkpoint := range rep.checkpoints {
		if checkpoint.Sequence < h || h == uint64(0) {
			h = checkpoint.Sequence
		}
	}

	var s uint64
	for _, summary := range summaries {
		if summary.Sequence < s || s == uint64(0) {
			s = summary.Sequence
		}
		if summary.Sequence > h {
			valid := false
			for _, req := range rep.requests["view-change"] { //in
				if pb.EQ(req.Digest(), summary.Digest) {
					valid = true
					break
				}
			}
			if !valid {
				return
			}
		}
	}

	if h < s {
		return
	}

	// Process new view
	rep.activeView = true

	for _, summary := range summaries {

		if rep.ID != reqNewView.Replica {
			req := pb.ToRequestPrepare(
				reqNewView.View,
				summary.Sequence,
				summary.Digest,
				rep.ID) // the backup sends/logs prepare

			go func() {
				if !rep.hasRequest(req) {
					rep.requestChan <- req
				}
			}()
			if summary.Sequence <= h {
				continue
			}

			if !rep.hasRequest(req) {
				rep.logRequest(req)
			}
		} else {
			if summary.Sequence <= h {
				break
			}
		}

		req := pb.ToRequestPreprepare(
			reqNewView.View,
			summary.Sequence,
			summary.Digest,
			reqNewView.Replica) // new primary pre-prepares

		if !rep.hasRequest(req) {
			rep.logRequest(req)
		}
	}

	var maxSequence uint64
	for _, req := range rep.requests["pre-prepare"] {
		reqPrePrepare := req.GetPreprepare()
		if reqPrePrepare.Sequence > maxSequence {
			maxSequence = reqPrePrepare.Sequence
		}
	}
	rep.sequence = maxSequence
	return true
}

func (rep *Replica) prePrepBySequence(sequence uint64) []*pb.Entry {
	var view uint64
	var requests []*pb.Request
	for _, req := range rep.requests["pre-prepare"] {
		v := req.GetPreprepare().View
		s := req.GetPreprepare().Sequence
		if v >= view && s == sequence {
			view = v
			requests = append(requests, req)
		}
	}
	if requests == nil {
		return nil
	}
	var prePreps []*pb.Entry
	for _, req := range requests {
		v := req.GetPreprepare().View
		if v == view {
			s := req.GetPreprepare().Sequence
			d := req.GetPreprepare().Digest
			prePrep := pb.ToEntry(s, d, v)
			prePreps = append(prePreps, prePrep)
		}
	}
FOR_LOOP:
	for _, prePrep := range prePreps {
		for _, req := range rep.allRequests() { //TODO: optimize
			if pb.EQ(req.Digest(), prePrep.Digest) {
				continue FOR_LOOP
			}
		}
		return nil
	}
	return prePreps
}

func (rep *Replica) prepBySequence(sequence uint64) ([]*pb.Entry, []*pb.Entry) {

	prePreps := rep.prePrepBySequence(sequence)

	if prePreps == nil {
		return nil, nil
	}

	var preps []*pb.Entry

FOR_LOOP:
	for _, prePrep := range prePreps {

		view := prePrep.View
		digest := prePrep.Digest

		replicas := make(map[uint64]int)
		for _, req := range rep.requests["prepare"] {
			reqPrepare := req.GetPrepare()
			v := reqPrepare.View
			s := reqPrepare.Sequence
			d := reqPrepare.Digest
			if v == view && s == sequence && pb.EQ(d, digest) {
				r := reqPrepare.Replica
				replicas[r]++
				if rep.twoThirds(replicas[r]) {
					prep := pb.ToEntry(s, d, v)
					preps = append(preps, prep)
					continue FOR_LOOP
				}
			}
		}
		return prePreps, nil
	}

	return prePreps, preps
}

/*
func (rep *Replica) prepBySequence(sequence uint64) *pb.Entry {
	var REQ *pb.Request
	var view uint64
	// Looking for a commit that
	// replica sent with sequence#
	for _, req := range rep.requests["commit"] {
		v := req.GetCommit().View
		s := req.GetCommit().Sequence
		r := req.GetCommit().Replica
		if v >= view && s == sequence && r == rep.ID {
			REQ = req
			view = v
		}
	}
	if REQ == nil {
		return nil
	}
	entry := pb.ToEntry(sequence, nil, view)
	return entry
}

func (rep *Replica) prePrepBySequence(sequence uint64) *pb.Entry {
	var REQ *pb.Request
	var view uint64
	// Looking for prepare/pre-prepare that
	// replica sent with matching sequence
	for _, req := range rep.requests["pre-prepare"] {
		v := req.GetPreprepare().View
		s := req.GetPreprepare().Sequence
		r := req.GetPreprepare().Replica
		if v >= view && s == sequence && r == rep.ID {
			REQ = req
			view = v
		}
	}
	for _, req := range rep.requests["prepare"] {
		v := req.GetPreprepare().View
		s := req.GetPreprepare().Sequence
		r := req.GetPreprepare().Replica
		if v >= view && s == sequence && r == rep.ID {
			REQ = req
			view = v
		}
	}
	if REQ == nil {
		return nil
	}
	var digest []byte
	switch REQ.Value.(type) {
	case *pb.Request_Preprepare:
		digest = REQ.GetPreprepare().Digest
	case *pb.Request_Prepare:
		digest = REQ.GetPrepare().Digest
	}
	entry := pb.ToEntry(sequence, digest, view)
	return entry
}
*/

func (rep *Replica) requestViewChange(view uint64) {

	if view != rep.view+1 {
		return
	}
	rep.view = view
	rep.activeView = false

	var prePreps []*pb.Entry
	var preps []*pb.Entry

	start := rep.lowWaterMark() + 1
	end := rep.highWaterMark()

	for s := start; s <= end; s++ {
		_prePreps, _preps := rep.prepBySequence(s)
		if _prePreps != nil {
			prePreps = append(prePreps, _prePreps...)
		}
		if _preps != nil {
			preps = append(preps, _preps...)
		}
	}

	sequence := rep.lowWaterMark()

	req := pb.ToRequestViewChange(
		view,
		sequence,
		rep.checkpoints, //
		preps,
		prePreps,
		rep.ID)

	rep.logRequest(req)

	go func() {
		rep.requestChan <- req
	}()

	rep.clearRequestsByView(view)
}

func (rep *Replica) createNewView(view uint64) (request *pb.Request) {

	// Returns RequestNewView if successful, else returns nil
	// create viewChanges
	viewChanges := make([]*pb.ViewChange, len(rep.pendingVC))

	for idx, _ := range viewChanges {
		req := rep.pendingVC[idx]
		viewchanger := req.GetViewchange().Replica
		vc := pb.ToViewChange(viewchanger, req.Digest())
		viewChanges[idx] = vc
	}

	var summaries []*pb.Summary
	var summary *pb.Summary

	start := rep.lowWaterMark() + 1
	end := rep.highWaterMark()

	// select starting checkpoint
FOR_LOOP_1:
	for seq := start; seq <= end; seq++ {

		overLWM := 0
		var digest []byte
		digests := make(map[string]int)

		for _, req := range rep.pendingVC {
			reqViewChange := req.GetViewchange()
			if reqViewChange.Sequence <= seq {
				overLWM++
			}
			for _, checkpoint := range reqViewChange.GetCheckpoints() {
				if checkpoint.Sequence == seq {
					d := checkpoint.Digest
					digests[string(d)]++
					if rep.oneThird(digests[string(d)]) {
						digest = d
						break
					}
				}
			}
			if rep.twoThirds(overLWM) && rep.oneThird(digests[string(digest)]) {
				summary = pb.ToSummary(seq, digest)
				continue FOR_LOOP_1
			}
		}
	}

	if summary == nil {
		return
	}

	summaries = append(summaries, summary)

	start = summary.Sequence
	end = start + CHECKPOINT_PERIOD*CONSTANT_FACTOR

	// select summaries
	// TODO: optimize
FOR_LOOP_2:
	for seq := start; seq <= end; seq++ {

		for _, REQ := range rep.pendingVC {

			sequence := REQ.GetViewchange().Sequence

			if sequence != seq {
				continue
			}

			var A1 []*pb.Request
			var A2 []*pb.Request

			view := REQ.GetViewchange().View
			digest := REQ.Digest()

		FOR_LOOP_3:
			for _, req := range rep.pendingVC {

				reqViewChange := req.GetViewchange()

				if reqViewChange.Sequence < sequence {
					preps := reqViewChange.GetPreps()
					for _, prep := range preps {
						if prep.Sequence != sequence {
							continue
						}
						if prep.View > view || (prep.View == view && !pb.EQ(prep.Digest, digest)) {
							continue FOR_LOOP_3
						}
					}
					A1 = append(A1, req)
				}
				prePreps := reqViewChange.GetPrepreps()
				for _, prePrep := range prePreps {
					if prePrep.Sequence != sequence {
						continue
					}
					if prePrep.View >= view && pb.EQ(prePrep.Digest, digest) {
						A2 = append(A2, req)
						continue FOR_LOOP_3
					}
				}
			}

			if rep.twoThirds(len(A1)) && rep.oneThird(len(A2)) {
				summary = pb.ToSummary(sequence, digest)
				summaries = append(summaries, summary)
				continue FOR_LOOP_2
			}
		}
	}

	request = pb.ToRequestNewView(view, viewChanges, summaries, rep.ID)
	return
}

func (rep *Replica) requestNewView(view uint64) {

	req := rep.createNewView(view)

	if req == nil || rep.hasRequest(req) {
		return
	}

	// Process new view

	success := rep.processNewView(req)

	if !success {
		return
	}

	rep.logRequest(req)

	go func() {
		rep.requestChan <- req
	}()

}
