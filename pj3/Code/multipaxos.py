import os
import sys
import socket
import time
import json
import collections
from twisted.internet import reactor, protocol
class PaxosMessage (object):
    from_uid = None

class Prepare (PaxosMessage):
    '''
    Prepare messages should be broadcast to all Acceptors.
    '''
    def __init__(self, from_uid, proposal_id):
        self.from_uid    = from_uid
        self.proposal_id = proposal_id
        
class Promise (PaxosMessage):
    '''
    Promise messages should be sent to at least the Proposer specified in
    the proposer_uid field
    '''
    def __init__(self, from_uid, proposer_uid, proposal_id, last_accepted_id, last_accepted_value):
        self.from_uid             = from_uid
        self.proposer_uid         = proposer_uid
        self.proposal_id          = proposal_id
        self.last_accepted_id     = last_accepted_id
        self.last_accepted_value  = last_accepted_value

        
class Accept (PaxosMessage):
    '''
    Accept messages should be broadcast to all Acceptors
    '''
    def __init__(self, from_uid, proposal_id, proposal_value):
        self.from_uid       = from_uid
        self.proposal_id    = proposal_id
        self.proposal_value = proposal_value

class Messenger(protocol.DatagramProtocol):

    def __init__(self, uid, peer_addresses, replicated_val):
        self.addrs          = dict(peer_addresses)
        self.replicated_val = replicated_val
        # provide two-way mapping between endpoints and server names
        for k,v in list(self.addrs.items()):
            self.addrs[v] = k

        reactor.listenUDP( peer_addresses[uid][1], self )
       
    def startProtocol(self):
        self.replicated_val.set_messenger(self)
        
    def datagramReceived(self, packet, from_addr):
        try:            
            message_type, data = packet.split(' ', 1)
            if message_type == 'propose':
                self.replicated_val.propose_update( data )
            else:
                from_uid = self.addrs[from_addr]
                print 'rcv', from_uid, ':', packet
                # Dynamically search the class for a method to handle this message
                handler = getattr(self.replicated_val, 'receive_' + message_type, None)
                if handler:
                    kwargs = json.loads(data)
                    for k in kwargs.keys():
                        if k.endswith('_id') and kwargs[k] is not None:
                            # JSON encodes the proposal ids as lists,
                            # composable-paxos requires requires ProposalID instances
                            kwargs[k] = ProposalID(*kwargs[k])
                    handler(from_uid, **kwargs)
        except Exception:
            print 'Error processing packet: ', packet
            import traceback
            traceback.print_exc()
            
    def _send(self, to_uid, message_type, **kwargs):
        msg = '{0} {1}'.format(message_type, json.dumps(kwargs))
        print 'snd', to_uid, ':', msg
        self.transport.write(msg, self.addrs[to_uid])

    def send_sync_request(self, peer_uid, instance_number):
        self._send(peer_uid, 'sync_request', instance_number=instance_number)

    def send_catchup(self, peer_uid, instance_number, current_value):
        self._send(peer_uid, 'catchup', instance_number = instance_number,
                                        current_value   = current_value)

    def send_nack(self, peer_uid, instance_number, proposal_id, promised_proposal_id):
        self._send(peer_uid, 'nack', instance_number      = instance_number,
                                     proposal_id          = proposal_id,
                                     promised_proposal_id = promised_proposal_id)

    def send_prepare(self, peer_uid, instance_number, proposal_id):
        self._send(peer_uid, 'prepare', instance_number = instance_number,
                                        proposal_id     = proposal_id)

    def send_promise(self, peer_uid, instance_number, proposal_id, last_accepted_id, last_accepted_value):
        self._send(peer_uid, 'promise',  instance_number     = instance_number,
                                         proposal_id         = proposal_id,
                                         last_accepted_id    = last_accepted_id,
                                         last_accepted_value = last_accepted_value )

    def send_accept(self, peer_uid, instance_number, proposal_id, proposal_value):
        self._send(peer_uid, 'accept', instance_number = instance_number,
                                       proposal_id     = proposal_id,
                                       proposal_value  = proposal_value)

    def send_accepted(self, peer_uid, instance_number, proposal_id, proposal_value):
        self._send(peer_uid, 'accepted', instance_number = instance_number,
                                         proposal_id     = proposal_id,
                                         proposal_value  = proposal_value)