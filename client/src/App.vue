<script setup>
import { ref, onMounted, onUnmounted, nextTick } from 'vue'

const isLoggedIn = ref(false)
const userId = ref('')
const deviceId = ref('')
const chatId = ref('')
const newMessage = ref('')
const messages = ref([])
const socket = ref(null)
const isConnected = ref(false)
const messagesContainer = ref(null)

function uuidv4() {
  return "10000000-1000-4000-8000-100000000000".replace(/[018]/g, c =>
    (+c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> +c / 4).toString(16)
  );
}

// Generate random IDs for convenience
const generateUUID = () => {
  return uuidv4()
}

onMounted(() => {
  userId.value = generateUUID()
  deviceId.value = generateUUID()
  chatId.value = '00000000-0000-0000-0000-000000000000' // Default chat
})

const connect = () => {
  if (!userId.value || !deviceId.value) return

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const wsUrl = `ws://localhost:8080/ws?user_id=${userId.value}&device_id=${deviceId.value}`

  socket.value = new WebSocket(wsUrl)

  socket.value.onopen = () => {
    isConnected.value = true
    isLoggedIn.value = true
    console.log('Connected to WebSocket')
    startHeartbeat()
    
    // Sync messages
    syncMessages()
    // Fetch members
    fetchMembers()
  }

  socket.value.onmessage = (event) => {
    try {
      // Handle heartbeat response
      if (event.data === 'pong') {
        // console.log('pong')
        return
      }

      const data = JSON.parse(event.data)
      console.log('WS Received:', data)
      
      if (data.type === 'MESSAGE_CREATED') {
        const msg = data.payload
        console.log('Processing Message:', msg)
        // Check if we already have it (deduplication)
        if (!messages.value.find(m => m.id === msg.id)) {
          console.log('Pushing to messages')
          messages.value.push(msg)
          scrollToBottom()
          if (msg.sender_id !== userId.value) {
             markDelivered(msg.chat_id, msg.id)
          }
        } else {
          console.log('Duplicate message ignored')
        }
      } else if (data.type === 'USER_JOINED') {
        const payload = data.payload
        // Add system message
        messages.value.push({
          id: uuidv4(),
          type: 'system',
          content: `User ${payload.user_id} joined the chat`,
          created_at: new Date().toISOString()
        })
        scrollToBottom()
        fetchMembers(chatId.value) // Refresh members list, pass chatId
      } else if (data.type === 'MESSAGE_READ') {
        const payload = JSON.parse(atob(data.payload))
        // Update status of messages in this chat up to last_read_message_id
        if (chatId.value === payload.chat_id) {
           // Find the message to get its timestamp (or index)
           // Simpler: just mark all messages in this chat as read if they are ours
           // In a real app, we'd check timestamps or IDs.
           // For now, let's just mark all "sent" or "delivered" messages as "read"
           messages.value.forEach(m => {
             if (m.sender_id !== userId.value) return // Only update own messages status
             // In real app, check m.id <= payload.last_read_message_id
             m.status = 'read'
           })
        }
      } else if (data.type === 'MESSAGE_DELIVERED') {
        const payload = data.payload
        if (chatId.value === payload.chat_id) {
           messages.value.forEach(m => {
             if (m.sender_id !== userId.value) return
             console.log('Marking message as delivered', m)
             if (m.status === 'sent') { // Assuming 'sent' is the initial status for outgoing messages
                m.status = 'delivered'
             }
           })
        }
      }
    } catch (e) {
      console.error('Failed to parse message:', e)
    }
  }

  socket.value.onclose = () => {
    isConnected.value = false
    console.log('Disconnected')
    stopHeartbeat()
    // Reconnect logic could go here
  }

  socket.value.onerror = (error) => {
    console.error('WebSocket error', error)
  }
}

const members = ref([])

const fetchMembers = async () => {
  if (!chatId.value) return
  try {
    const response = await fetch(`http://localhost:8080/chats/members?chat_id=${chatId.value}`)
    if (response.ok) {
      members.value = await response.json()
    }
  } catch (e) {
    console.error('Failed to fetch members:', e)
  }
}

const markDelivered = async (chatId, messageId) => {
  if (!userId.value) return
  try {
    await fetch('http://localhost:8080/messages/delivered', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        user_id: userId.value,
        last_message_id: messageId
      })
    })
  } catch (e) {
    console.error("Failed to send delivery receipt", e)
  }
}

const syncMessages = async () => {
  if (!chatId.value) return
  
  // Find last message ID
  const lastMsg = messages.value[messages.value.length - 1]
  const afterId = lastMsg && lastMsg.type !== 'system' ? lastMsg.id : ''
  
  try {
    const response = await fetch(`http://localhost:8080/messages/sync?chat_id=${chatId.value}&after_id=${afterId}`)
    if (response.ok) {
      const newMessages = await response.json()
      if (newMessages && newMessages.length > 0) {
        // Filter out duplicates just in case
        const unique = newMessages.filter(nm => !messages.value.find(m => m.id === nm.id))
        messages.value.push(...unique)
        scrollToBottom()
      }
    }
  } catch (e) {
    console.error('Sync failed:', e)
  }
}

const sendMessage = async () => {
  if (!newMessage.value.trim() || !userId.value || !chatId.value) return

  const msg = {
    chat_id: chatId.value,
    sender_id: userId.value,
    content: newMessage.value
  }

  try {
    const response = await fetch('http://localhost:8080/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(msg)
    })

    if (response.ok) {
      newMessage.value = ''
      // Message will be received via WebSocket
    } else {
      console.error('Failed to send message')
    }
  } catch (e) {
    console.error('Error sending message:', e)
  }
}

const scrollToBottom = () => {
  nextTick(() => {
    if (messagesContainer.value) {
      messagesContainer.value.scrollTop = messagesContainer.value.scrollHeight
    }
  })
}

const disconnect = () => {
  if (socket.value) {
    socket.value.close()
    socket.value = null
  }
  isLoggedIn.value = false
  isConnected.value = false
}

// Heartbeat logic
let heartbeatInterval

const startHeartbeat = () => {
  stopHeartbeat()
  heartbeatInterval = setInterval(() => {
    if (socket.value && socket.value.readyState === WebSocket.OPEN) {
      socket.value.send('ping')
    }
  }, 30000) // 30s
}

const stopHeartbeat = () => {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval)
    heartbeatInterval = null
  }
}

onUnmounted(() => {
  disconnect()
})
</script>

<template>
  <div class="container">
    <div v-if="!isLoggedIn" class="login-card">
      <h1>Chat Login</h1>
      <div class="input-group">
        <label>User ID</label>
        <input v-model="userId" type="text" placeholder="UUID" />
        <button @click="userId = generateUUID()" class="secondary">Generate</button>
      </div>
      <div class="input-group">
        <label>Device ID</label>
        <input v-model="deviceId" type="text" placeholder="UUID" />
        <button @click="deviceId = generateUUID()" class="secondary">Generate</button>
      </div>
      <div class="input-group">
        <label>Chat ID</label>
        <input v-model="chatId" type="text" placeholder="UUID" />
        <button @click="chatId = generateUUID()" class="secondary">Generate</button>
      </div>
      <button @click="connect" class="primary" :disabled="!userId || !deviceId || !chatId">Connect</button>
    </div>

    <div v-else class="chat-interface">
      <header>
        <div class="status">
          <span class="indicator" :class="{ online: isConnected }"></span>
          {{ isConnected ? 'Online' : 'Offline' }}
        </div>
        <div class="info">
          User: {{ userId.slice(0, 8) }}... | Chat: {{ chatId.slice(0, 8) }}...
        </div>
        <button @click="disconnect" class="danger small">Disconnect</button>
      </header>

      <div class="messages" ref="messagesContainer">
        <div 
          v-for="msg in messages" 
          :key="msg.id" 
          :class="['message-bubble', msg.sender_id === userId ? 'my-message' : '']"
        >
          <div v-if="msg.type === 'system'" class="system-message">
            {{ msg.content }}
          </div>
          <div v-else>
            <div class="sender" v-if="msg.sender_id !== userId">{{ msg.sender_id.slice(0, 8) }}</div>
            <div class="content">{{ msg.content }}</div>
            <div class="meta">
              <span class="time">{{ new Date(msg.created_at).toLocaleTimeString() }}</span>
              <span v-if="msg.sender_id === userId" class="text-xs ml-1">
                <span v-if="msg.status === 'read'" class="text-blue-500">✓✓</span>
                <span v-else-if="msg.status === 'delivered'" class="text-gray-500">✓✓</span>
                <span v-else class="text-gray-400">✓</span>
              </span>
            </div>
          </div>
        </div>
      </div>
      
      <div class="input-area">
        <input 
          v-model="newMessage" 
          @keyup.enter="sendMessage" 
          type="text" 
          placeholder="Type a message..." 
          :disabled="!isConnected"
        />
        <button @click="sendMessage" :disabled="!isConnected || !newMessage">Send</button>
      </div>
    </div>
    
    <div v-if="isLoggedIn" class="members-sidebar">
      <h3>Members</h3>
      <ul>
        <li v-for="member in members" :key="member.id">
          {{ member.username || member.id.slice(0, 8) }}
          <span v-if="member.id === userId">(You)</span>
        </li>
      </ul>
    </div>
  </div>
</template>

<style scoped>
.container {
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 100vh;
  background-color: #1a1a1a;
  color: #ffffff;
  font-family: 'Inter', sans-serif;
  gap: 20px;
}

.members-sidebar {
  width: 200px;
  background: #2a2a2a;
  padding: 1rem;
  border-radius: 12px;
  height: 80vh;
  overflow-y: auto;
}

.members-sidebar h3 {
  margin-top: 0;
  border-bottom: 1px solid #444;
  padding-bottom: 0.5rem;
}

.members-sidebar ul {
  list-style: none;
  padding: 0;
}

.members-sidebar li {
  padding: 0.5rem 0;
  border-bottom: 1px solid #333;
  font-size: 0.9rem;
}

/* ... existing styles ... */
.login-card {
  background: #2a2a2a;
  padding: 2rem;
  border-radius: 12px;
  width: 100%;
  max-width: 400px;
  box-shadow: 0 8px 24px rgba(0,0,0,0.5);
}

.form-group {
  margin-bottom: 1rem;
}

label {
  display: block;
  margin-bottom: 0.5rem;
  font-size: 0.9rem;
  color: #aaa;
}

input {
  width: 100%;
  padding: 0.8rem;
  border-radius: 6px;
  border: 1px solid #444;
  background: #333;
  color: white;
  font-size: 1rem;
}

input:focus {
  outline: none;
  border-color: #646cff;
}

.btn-primary {
  width: 100%;
  padding: 0.8rem;
  background: #646cff;
  color: white;
  border: none;
  border-radius: 6px;
  font-weight: bold;
  cursor: pointer;
  transition: background 0.2s;
}

.btn-primary:hover {
  background: #535bf2;
}

.btn-secondary {
  padding: 0.5rem 1rem;
  background: #444;
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
}

.chat-interface {
  width: 100%;
  max-width: 600px;
  height: 80vh;
  background: #2a2a2a;
  border-radius: 12px;
  display: flex;
  flex-direction: column;
  box-shadow: 0 8px 24px rgba(0,0,0,0.5);
}

header {
  padding: 1rem;
  border-bottom: 1px solid #444;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.status {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.9rem;
}

.indicator {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #ff4444;
}

.indicator.online {
  background: #44ff44;
}

.messages {
  flex: 1;
  overflow-y: auto;
  padding: 1rem;
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.empty-state {
  text-align: center;
  color: #666;
  margin-top: 2rem;
}

.message-bubble {
  max-width: 80%;
  padding: 0.8rem;
  border-radius: 12px;
  background: #333;
  align-self: flex-start;
}

.my-message {
  align-self: flex-end;
  background: #646cff;
}

.sender {
  font-size: 0.75rem;
  opacity: 0.7;
  margin-bottom: 0.2rem;
}

.time {
  font-size: 0.7rem;
  opacity: 0.5;
  text-align: right;
  margin-top: 0.2rem;
}

.input-area {
  padding: 1rem;
  border-top: 1px solid #444;
  display: flex;
  gap: 0.5rem;
}

.input-area input {
  flex: 1;
}

.input-area .btn-primary {
  width: auto;
}
</style>
