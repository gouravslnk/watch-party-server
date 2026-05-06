/**
 * WatchParty Socket.IO Server
 * Computer Networks Project — Client-Server Architecture
 * 
 * This server acts as the central signaling/sync hub.
 * The HOST (admin) is the source of truth for video state.
 * All CLIENTS receive sync updates from the host via this server.
 * 
 * Architecture:
 *   Host --[player-control]--> Server --[player-update]--> All Clients
 *   Client --[request-video-state]--> Server --[request-video-state]--> Host
 *   Host --[video-state-response]--> Server --[sync-video-state]--> Requesting Client
 */

import { createServer } from "http";
import { Server } from "socket.io";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
dotenv.config();

const PORT = process.env.PORT || 3001;
const SUPABASE_URL = process.env.VITE_SUPABASE_URL;
const SUPABASE_KEY = process.env.VITE_SUPABASE_ANON_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const httpServer = createServer();
const io = new Server(httpServer, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
  transports: ["websocket", "polling"],
});

// In-memory room registry: roomCode -> { members: Map<socketId, username>, admin: username }
const rooms = new Map();

function getRoomMembers(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return [];
  return Array.from(room.members.values());
}

function findAdminSocket(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return null;

  for (const [socketId, username] of room.members) {
    if (username === room.admin) {
      return socketId;
    }
  }
  return null;
}

io.on("connection", (socket) => {
  console.log(`[+] Client connected: ${socket.id}`);

  let currentRoom = null;
  let currentUsername = null;

  // ─── JOIN ROOM ────────────────────────────────────────────────────────────
  socket.on("join-room", async ({ roomCode, username }) => {
    if (!roomCode || !username) return;

    currentRoom = roomCode;
    currentUsername = username;

    socket.join(roomCode);

    // Ensure room exists in memory
    if (!rooms.has(roomCode)) {
      // Fetch admin from Supabase
      const { data } = await supabase
        .from("rooms")
        .select("admin")
        .eq("roomCode", roomCode)
        .maybeSingle();

      rooms.set(roomCode, {
        members: new Map(),
        admin: data?.admin || username, // fallback to first joiner
      });
    }

    const room = rooms.get(roomCode);
    room.members.set(socket.id, username);

    const membersList = getRoomMembers(roomCode);
    const count = membersList.length;

    // Notify everyone in the room
    io.to(roomCode).emit("user-joined", {
      username,
      members: count,
      membersList,
    });

    // Send presence state to the new joiner
    socket.emit("presence-sync", { members: count, membersList });

    // Ask host to send video state to this new client
    const adminSocketId = findAdminSocket(roomCode);
    if (adminSocketId && adminSocketId !== socket.id) {
      io.to(adminSocketId).emit("request-video-state", {
        requestingSocketId: socket.id,
      });
    }

    console.log(`[ROOM ${roomCode}] ${username} joined (${count} total)`);
  });

  // ─── HOST → CLIENTS: Player control ──────────────────────────────────────
  socket.on("player-control", (data) => {
    if (!currentRoom) return;

    const room = rooms.get(currentRoom);
    if (!room) return;

    // Only admin can emit player controls
    if (room.members.get(socket.id) !== room.admin) return;

    // Broadcast to everyone EXCEPT host
    socket.to(currentRoom).emit("player-update", {
      ...data,
      username: currentUsername,
    });
  });

  // ─── HOST → SERVER → NEW CLIENT: State sync ──────────────────────────────
  socket.on("video-state-response", ({ requestingSocketId, currentTime, isPlaying }) => {
    // Host sends current video state; server forwards to the requesting client only
    io.to(requestingSocketId).emit("sync-video-state", {
      currentTime,
      isPlaying,
    });
  });

  // ─── HOST: Change video ───────────────────────────────────────────────────
  socket.on("change-video", (data) => {
    if (!currentRoom) return;
    const room = rooms.get(currentRoom);
    if (!room || room.members.get(socket.id) !== room.admin) return;

    // Notify all clients in the room about new video
    socket.to(currentRoom).emit("video-changed", data);
    console.log(`[ROOM ${currentRoom}] Video changed by host`);
  });

  // ─── CHAT ─────────────────────────────────────────────────────────────────
  socket.on("send-message", (message) => {
    if (!currentRoom || !currentUsername || !message) return;

    socket.to(currentRoom).emit("new-message", {
      username: currentUsername,
      message: message.toString().slice(0, 500),
    });
  });

  // ─── REACTIONS ────────────────────────────────────────────────────────────
  socket.on("send-reaction", (emoji) => {
    if (!currentRoom || !currentUsername) return;
    io.to(currentRoom).emit("new-reaction", { username: currentUsername, emoji });
  });

  // ─── LEAVE ROOM ──────────────────────────────────────────────────────────
  socket.on("leave-room", async () => {
    await handleLeave();
  });

  async function handleLeave() {
    if (!currentRoom) return;

    const room = rooms.get(currentRoom);
    if (!room) return;

    const wasAdmin = room.members.get(socket.id) === room.admin;
    room.members.delete(socket.id);

    socket.leave(currentRoom);

    const membersList = getRoomMembers(currentRoom);
    const count = membersList.length;

    if (wasAdmin || count === 0) {
      // Admin left → delete room from Supabase & memory
      await supabase.from("rooms").delete().eq("roomCode", currentRoom);
      rooms.delete(currentRoom);

      io.to(currentRoom).emit("room-ended", {
        message: "The host ended the party.",
      });
      console.log(`[ROOM ${currentRoom}] Deleted (host left)`);
    } else {
      io.to(currentRoom).emit("user-left", {
        username: currentUsername,
        members: count,
        membersList,
      });
      console.log(`[ROOM ${currentRoom}] ${currentUsername} left (${count} remaining)`);
    }

    currentRoom = null;
    currentUsername = null;
  }

  // ─── DISCONNECT ───────────────────────────────────────────────────────────
  socket.on("disconnect", async () => {
    console.log(`[-] Client disconnected: ${socket.id}`);
    await handleLeave();
  });
});

httpServer.listen(PORT, () => {
  console.log(`\n🎬 WatchParty Socket Server running on port ${PORT}`);
  console.log(`   Architecture: Host → Server → Clients (Socket.IO)`);
  console.log(`   Backend: Supabase (room persistence)\n`);
});
