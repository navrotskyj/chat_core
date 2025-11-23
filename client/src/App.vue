<script setup>
import { ref, onMounted, onUnmounted, nextTick } from 'vue'

const isLoggedIn = ref(false)
const userId = ref('')
const deviceId = ref('')
const chatId = ref('')
const message = ref('')
const messages = ref([])
const socket = ref(null)
const isConnected = ref(false)
const messagesContainer = ref(null)

// Generate random IDs for convenience
const generateUUID = () => {
  return crypto.randomUUID()
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
  }

  socket.value.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data)
      messages.value.push(msg)
      scrollToBottom()
    } catch (e) {
      console.error('Failed to parse message', e)
    }
  }

  socket.value.onclose = () => {
    isConnected.value = false
    console.log('Disconnected from WebSocket')
  }

  socket.value.onerror = (error) => {
    console.error('WebSocket error', error)
  }
}

const sendMessage = async () => {
  if (!message.value.trim()) return

  const payload = {
    chat_id: chatId.value,
    sender_id: userId.value,
    content: message.value
  }

  try {
    const response = await fetch('http://localhost:8080/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    })

    if (!response.ok) {
      throw new Error('Failed to send message')
    }

    // Optimistic update or wait for WS? 
    // The backend broadcasts to sender too via WS if connected to same node?
    // My backend logic: BroadcastToChat -> BroadcastToUser -> finds connections.
    // Yes, it should come back via WS.
    message.value = ''
  } catch (e) {
    console.error(e)
    alert('Error sending message')
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
  }
  isLoggedIn.value = false
}
</script>

<template>
  <div class="container">
    <div v-if="!isLoggedIn" class="login-card">
      <h1>Chat Core Client</h1>
      <div class="form-group">
        <label>User ID</label>
        <input v-model="userId" type="text" placeholder="UUID" />
      </div>
      <div class="form-group">
        <label>Device ID</label>
        <input v-model="deviceId" type="text" placeholder="UUID" />
      </div>
      <div class="form-group">
        <label>Chat ID</label>
        <input v-model="chatId" type="text" placeholder="UUID" />
      </div>
      <button @click="connect" class="btn-primary">Connect</button>
    </div>

    <div v-else class="chat-interface">
      <header>
        <div class="status">
          <span class="indicator" :class="{ online: isConnected }"></span>
          {{ isConnected ? 'Connected' : 'Disconnected' }}
        </div>
        <button @click="disconnect" class="btn-secondary">Disconnect</button>
      </header>
      
      <div class="messages" ref="messagesContainer">
        <div v-if="messages.length === 0" class="empty-state">
          No messages yet
        </div>
        <div 
          v-for="msg in messages" 
          :key="msg.id" 
          class="message-bubble"
          :class="{ 'my-message': msg.sender_id === userId }"
        >
          <div class="sender">{{ msg.sender_id.slice(0, 8) }}...</div>
          <div class="content">{{ msg.content }}</div>
          <div class="time">{{ new Date(msg.created_at).toLocaleTimeString() }}</div>
        </div>
      </div>

      <div class="input-area">
        <input 
          v-model="message" 
          @keyup.enter="sendMessage" 
          type="text" 
          placeholder="Type a message..." 
        />
        <button @click="sendMessage" class="btn-primary">Send</button>
      </div>
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
}

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
