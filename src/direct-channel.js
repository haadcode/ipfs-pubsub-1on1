'use strict'

const path = require('path')
const EventEmitter = require('events')
const PROTOCOL = require('./protocol')
const encode = require('./encoding')
const waitForPeers = require('./wait-for-peers')
const getPeerID = require('./get-peer-id')

/**
 * Communication channel over Pubsub between two IPFS nodes
 */
class DirectChannel extends EventEmitter {
  constructor (ipfs, receiverID) {
    super()

    // IPFS instance to use internally
    this._ipfs = ipfs

    if (!ipfs.pubsub) {
      throw new Error('This IPFS node does not support pubsub.')
    }

    this._closed = false
    this._isClosed = () => this._closed
    this._receiverID = receiverID

    if (!this._receiverID) {
      throw new Error('Receiver ID was undefined')
    }
    // See _setup() for more state initialization
  }

  /**
   * Channel ID
   * @return {[String]} Channel's ID
   */
  get id () {
    return this._id
  }

  /**
   * Peers participating in this channel
   * @return {[Array]} Array of peer IDs participating in this channel
   */
  get peers () {
    return this._peers
  }

  async connect () {
    await waitForPeers(this._ipfs, [this._receiverID], this._id, this._isClosed)
  }

  /**
   * Send a message to the other peer
   * @param  {[Any]} message Payload
   */
  async send (message) {
    if (this._closed) return
    let m = encode(message)
    await this._ipfs.pubsub.publish(this._id, m)
  }

  /**
   * Close the channel
   */
  close () {
    this._closed = true
    this.removeAllListeners('message')
    this._ipfs.pubsub.unsubscribe(this._id, this._messageHandler)
  }

  async _setup () {
    this._senderID = await getPeerID(this._ipfs)

    // Channel's participants
    this._peers = Array.from([this._senderID, this._receiverID]).sort()

    // ID of the channel is "<peer1 id>/<peer 2 id>""
    this._id = '/' + PROTOCOL + '/' + this._peers.join('/')

    // Function to use to handle incoming messages
    this._messageHandler = message => {
      // Make sure the message is coming from the correct peer
      const isValid = message && message.from === this._receiverID
      // Filter out all messages that didn't come from the second peer
      if (isValid) {
        this.emit('message', message)
      }
    }
  }

  async _openChannel () {
    this._closed = false
    await this._setup()
    await this._ipfs.pubsub.subscribe(this._id, this._messageHandler)
  }

  static async open (ipfs, receiverID) {
    const channel = new DirectChannel(ipfs, receiverID)
    await channel._openChannel()
    return channel
  }
}

module.exports = DirectChannel
