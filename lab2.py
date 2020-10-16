"""
Course:  CPSC 5520-01, Seattle University
Author:  JungBok Cho
Version: 1.0

I created this program based on the template offered by Professor Kevin Lundeen.
I did both Extra Credits. To check the PROBE Message Extra Credit, please remove
the comment of self.probing() in the run() function. To check the Feigning
Failure Extra Credit, please remove the comment of self.check_timeouts()
in the run() function.
"""

import selectors
from datetime import datetime, timedelta
import time as t
from enum import Enum
import pickle
import socket
import sys
import random

BUF_SZ = 1024                  # TCP receive buffer size
DEFAULT_BOD = '1995-01-20'     # Default date of birth
PEER_DIGITS = 100              # Max Socket number
BACKLOG = 100                  # Number of backlog
CHECK_INTERVAL = 0.2           # To wait until some registered file objects
                               # become ready
ASSUME_FAILURE_TIMEOUT = 2000  # In milliseconds. If it takes less than 2 seconds
                               # to receive message, it will take the message.
                               # If it takes longer than 2 seconds, it will
                               # reject the message


class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'
    PROBE = 'PROBE'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their
                                         # connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY,
                            State.SEND_OK, State.PROBE)


class Lab2(object):
    """
    Dynamically elect the biggest bully from a group of distributed
    computer processes. The process with the highest DOB from amongst
    the non-failed processes is selected as the bully. If some has the
    same highest DOB, it will check which one has the highest SU ID.
    """

    def __init__(self, gcd_address, next_birthday, su_id):
        """
        Constructs a Lab2 object to talk to the given Group Coordinator
        and initialize instance variables

        :param gcd_address: GCD address to contact - (host, port)
        :param next_birthday: User's next birthday
        :param su_id: User's unique SU ID
        """
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (next_birthday - datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))  # To compare between users
        self.connectedMembers = {}  # Store only connected members
        self.members = {}  # Store every member
        self.states = {}   # Store a state of each member
        self.bully = None  # Biggest bully
        self.nextFailureTime = timedelta(0, random.randint(0, 10000) / 1000)
        self.startTime = datetime.now()  # Starting time for timeout
        self.endTime = datetime.now()  # Ending time for timeout
        self.ignoreIfOkExpired = False  # Check if time for ok message is expired
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()

    def run(self):
        """Start to find the biggest bully"""
        print('STARTING WORK for pid {} on {}'.format(self.pid, self.listener_address))
        self.join_group()
        self.selector.register(self.listener, selectors.EVENT_READ)
        self.start_election('at startup')
        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask == selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)
            # self.check_timeouts()  # Feigning Failure
            # self.probing()  # PROBE Message 

    def accept_peer(self):
        """Accept if peers wants to connect"""
        try:
            conn, addr = self.listener.accept()
            self.set_state(State.WAITING_FOR_ANY_MESSAGE, conn)
        except Exception as err:
            print(err)

    def send_message(self, peer):
        """
        Send the queued message to the given peer (based on its current state)

        :param peer: The socket to send a message
        """
        state = self.get_state(peer)
        print('{}: sending {} [{}]'.format(self.pr_sock(peer), state.value, self.pr_now()))
        try:
            if state.value == 'COORDINATOR':
                # If we found a leader, we send connected members only
                self.send(peer, state.value, self.connectedMembers)
            elif state.value == 'PROBE':
                self.send(peer, state.value, None)
            else:
                self.send(peer, state.value, self.members)
        except ConnectionError as err:
            print(err)
        except Exception as err:
            print(err)

        if state == state.SEND_ELECTION:
            self.set_state(State.WAITING_FOR_OK, peer, switch_mode=True)
        else:
            self.set_quiescent(peer)

    @staticmethod
    def findWinner(connectedMembers):
        """
        Find the leader among the connect members

        :param connectedMembers: Dictionary of connected members
        :return: Return the largest pid
        """
        keys = list(connectedMembers.keys())
        largest = keys[0]
        for member in keys:
            if largest < member:
                largest = member
        return largest

    def receive_message(self, peer):
        """
        Receive the queued message from the given peer (based on its current state)

        :param peer: The socket to receive a message
        """
        try:
            message_name = self.receive(peer)
        except Exception as err:
            print(err)

        if self.ignoreIfOkExpired:  # Check if time for ok message is expired
            print('self: No Returned OK Message - Keep the current leader')
            self.ignoreIfOkExpired = False
            self.set_quiescent(peer)
            return

        if message_name[0] != 'OK' and message_name[0] != 'PROBE':
            self.update_members(message_name[1])  # Update members

        if self.is_expired(peer):  # Operate this if time is expired
            if message_name[0] == 'OK':
                self.ignoreIfOkExpired = True
                self.declare_victory('No Returned OK Message')
                if len(self.states) != 0:
                    self.set_quiescent(peer)
                return
            if message_name[0] == 'COORDINATOR':
                self.start_election('No Returned Victory Message')
                if len(self.states) != 0:
                    self.set_quiescent(peer)
                return

        # Check the Message
        if message_name[0] == 'ELECTION':
            self.set_state(State.SEND_OK, peer)
            if not self.is_election_in_progress():
                self.start_election('Got a VOTE card from lower-pid peer')
        elif message_name[0] == 'COORDINATOR':
            self.set_leader(self.findWinner(message_name[1]))
            print('self: Received COORDINATOR message')
            self.set_quiescent(peer)
            if len(self.states) != 0:  # Remove remaining states
                self.set_quiescent()
                keysStates = self.states.keys()
                for member in list(keysStates):
                    self.set_quiescent(member)
        elif message_name[0] == 'OK':
            print('self: Received OK message')
            if self.get_state() == State.WAITING_FOR_OK:
                self.set_state(State.WAITING_FOR_VICTOR)
            self.set_quiescent(peer)
        elif message_name[0] == 'PROBE':
            print('self: Probing received')
            self.set_state(State.SEND_OK, peer)

    def check_timeouts(self):
        """Check timeouts"""
        self.endTime = datetime.now()
        self.nextFailureTime -= (self.endTime - self.startTime)
        if self.nextFailureTime < timedelta(0, 0):
            sleepDuration = random.randint(1000, 4000)
            t.sleep(sleepDuration/1000)
            self.nextFailureTime = timedelta(0, random.randint(0, 10000) / 1000)
        self.startTime = datetime.now()

    def probing(self):
        """Check if the bully is still alive"""
        if self.bully is not None and self.bully != self.pid:
            peer = self.get_connection(self.bully)
            if peer is None:  # Start election if bully got disconnected
                self.start_election('bully got disconnected')
            else:
                randomNum = random.randint(500, 3000)
                t.sleep(randomNum/1000)
                self.set_state(State.PROBE, peer)
                print('self: Probing sent after waiting', randomNum/1000, 'seconds')

    def get_connection(self, member):
        """
        Get a connection of the member

        :param member: The member to connect
        :return: Return a socket if connected. Otherwise, return None
        """
        try:
            peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer.connect(self.members[member])
        except Exception as err:
            print('failed to connect to {}: {}'.format(member, err))
            return None
        else:
            return peer

    def is_election_in_progress(self):
        """
        Check if it is in Quiescent state

        :return: Return True if quiescent. Otherwise, return False
        """
        return self.get_state() != State.QUIESCENT

    def is_expired(self, peer=None, threshold=ASSUME_FAILURE_TIMEOUT):
        """
        Check if the time is expired
        
        :param peer: The peer socket to check
        :param threshold: It is in seconds. Need to convert to milliseconds
        :return: Return True if time is expired. Otherwise, return False
        """
        return datetime.now() - self.states[peer][1] > timedelta(0, threshold / 1000)

    def set_leader(self, new_leader):
        """
        Set a new Leader

        :param new_leader: New Leader to set
        """
        self.bully = new_leader

    def get_state(self, peer=None, detail=False):
        """
        Look up current state in state table.

        :param peer: Socket connected to peer process (None means self)
        :param detail: If True, then the state and timestamp are both returned
        :return: Return either the state or (state, timestamp) depending on detail
                 (not found gives (QUIESCENT, None))
        """
        if peer is None:
            peer = self
        status = self.states[peer] if peer in self.states else (State.QUIESCENT, None)
        return status if detail else status[0]

    def set_state(self, state, peer=None, switch_mode=False):
        """
        Change the current state in the state table

        :param state: The state to set
        :param peer:  The peer to change the state
        :param switch_mode: If True, it will modify mask in the peer's selector
        :return: After a peer is set to quiescent, stop this function.
                 Return nothing.
        """
        print('{}: {}'.format(self.pr_sock(peer), state.name))
        if peer is None:
            peer = self

        if state.is_incoming():
            mask = selectors.EVENT_READ
        else:
            mask = selectors.EVENT_WRITE

        # Set to Quiescent state
        if state == state.QUIESCENT:
            if peer in self.states:
                if peer != self:
                    self.selector.unregister(peer)
                del self.states[peer]
            if len(self.states) == 0:
                print('{} (leader: {})\n'.format(self.pr_now(), self.pr_leader()))
            return

        # If it is new, register the peer
        if peer != self and peer not in self.states:
            peer.setblocking(False)
            self.selector.register(peer, mask)
        elif switch_mode:  # If already registered, modify the mask
            self.selector.modify(peer, mask)
        self.states[peer] = (state, datetime.now())

        # Send message if the peer is Event_Write mode
        if mask == selectors.EVENT_WRITE:
            self.send_message(peer)

    def set_quiescent(self, peer=None):
        """
        Set the peer's state to Quiescent

        :param peer: The peer to be set quiescent
        """
        self.set_state(State.QUIESCENT, peer)

    def start_election(self, reason):
        """
        Start an election

        :param reason: The reason why it starts the election
        """
        print('Starting an election {}'.format(reason))
        self.set_leader(None)
        self.set_state(State.WAITING_FOR_OK)
        i_am_highest_bully = True
        for member in self.members:
            # Find higher bully
            if member > self.pid:
                peer = self.get_connection(member)
                if peer is None:
                    continue
                self.set_state(State.SEND_ELECTION, peer)
                i_am_highest_bully = False
        # Declare victory if I am the highest bully
        if i_am_highest_bully:
            self.declare_victory('no one is greater bully than me')

    def declare_victory(self, reason):
        """
        Declare victory. Initialize a dictionary of connected members

        :param reason: The reason why I declare victory
        """
        print('Victory by {} {}'.format(self.pid, reason))
        self.set_leader(self.pid)
        self.connectedMembers[self.pid] = self.listener_address
        for member in self.members:
            if member < self.pid:
                peer = self.get_connection(member)
                if peer is None:
                    continue
                self.connectedMembers[member] = self.members[member]
                self.set_state(State.SEND_VICTORY, peer)
        self.set_quiescent()

    def update_members(self, their_idea_of_membership):
        """
        Update members. It it is a new member, add to the members. If it is
        existing member, update the socket. Never delete members.

        :param their_idea_of_membership: Dictionary of members to use to update
        """
        for newMem in their_idea_of_membership:
            self.members[newMem] = their_idea_of_membership[newMem]
        print('Update members: ', self.members)

    @classmethod
    def send(cls, peer, message_name, message_data=None,
             wait_for_reply=False, buffer_size=BUF_SZ):
        """
        Send message helper

        :param peer: The socket to send message
        :param message_name: The Message Title
        :param message_data: Dictionary of members
        :param wait_for_reply: Check if it requires reply message
        :param buffer_size: Buffer size for message
        :return: Return receive message if wait_for_reply is True.
                 Otherwise, return nothing
        """
        message = message_name if message_data is None else (message_name, message_data)
        peer.sendall(pickle.dumps(message))
        if wait_for_reply:  # Operate it if it requires any reply
            return cls.receive(peer, buffer_size)

    @staticmethod
    def receive(peer, buffer_size=BUF_SZ):
        """
        Receive message helper

        :param peer: The socket to receive message
        :param buffer_size: Buffer size for message
        :return: Return the received date
                 (message_name, dictionary of updated member)
        """
        recvMessage = peer.recv(buffer_size)
        if not recvMessage:
            raise ValueError('socket closed')
        data = pickle.loads(recvMessage)
        if type(data) == str:
            data = (data, None)
        return data

    @staticmethod
    def start_a_server():
        """
        Start a server

        :return: Return server socket and its socket address
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(('localhost', 0))
        listener.listen(BACKLOG)
        return listener, listener.getsockname()

    def join_group(self):
        """Connect to GCD2 to get a dictionary of members"""
        gcd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        gcd.connect(self.gcd_address)
        self.members = self.send(gcd, 'JOIN', (self.pid, self.listener_address), True)
        print('Members: {}'.format(self.members))

    @staticmethod
    def pr_now():
        """
        Printing helper for current timestamp

        :return: Return current time
        """
        return datetime.now().strftime('%H:%M:%S.%f')

    def pr_sock(self, sock):
        """
        Printing helper for given socket

        :param sock: The socket to check
        :return: Return self if sock is None or self or equals to listener.
                 Otherwise, return the cpr_sock function
        """
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)

    @staticmethod
    def cpr_sock(sock):
        """
        Static version of helper for printing given socket

        :param sock: The socket to check
        :return: Return a string that shows a connection between sockets
        """
        l_port = sock.getsockname()[1] % PEER_DIGITS
        try:
            r_port = sock.getpeername()[1] % PEER_DIGITS
        except OSError:
            r_port = '???'
        return '{}->{} ({})'.format(l_port, r_port, id(sock))

    def pr_leader(self):
        """Printing helper for current leader's name"""
        return 'unknown' if self.bully is None \
            else ('self' if self.bully == self.pid else self.bully)


def setDOB():
    """
    Get the next birth day

    :return: Return the next birth day
    """
    now = datetime.now()

    if len(sys.argv) == 4:
        myDOB = datetime(now.year + 1, 1, 1)
    else:
        pieces = sys.argv[4].split('-')
        myDOB = datetime(now.year, int(pieces[1]), int(pieces[2]))
        if myDOB < now:  # if already passed, get birthday in the next year
            myDOB = datetime(myDOB.year + 1, int(pieces[1]), int(pieces[2]))
    return myDOB


if __name__ == '__main__':
    if not 4 <= len(sys.argv) <= 5:
        print("Usage: python lab2.py GCDHOST GCDPORT SUID [DOB]")
        exit(1)

    DOB = setDOB()
    print('Next Birthday: ', DOB)
    host, port, SUID = sys.argv[1:4]
    print('SU ID: ', SUID)
    lab2 = Lab2((host, port), DOB, int(SUID))
    lab2.run()
