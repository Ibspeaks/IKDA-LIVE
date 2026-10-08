const { createClient } = supabase;

const ikdaSupabase = createClient(
  SUPABASE_URL,
  SUPABASE_KEY
);

// Get room ID
const urlParams = new URLSearchParams(
  window.location.search
);

const roomId =
  urlParams.get("room") || "IKDA-DEMO";


// Unique participant ID
const participantId =
  crypto.randomUUID();


// WebRTC connections
const peers = {};


// Local microphone
let localStream = null;


// Supabase Realtime channel
let roomChannel = null;


// WebRTC servers
const rtcConfiguration = {

  iceServers: [

    {
      urls: "stun:stun.l.google.com:19302"
    },

    {
      urls: "stun:stun1.l.google.com:19302"
    },

    {
      urls: "stun:stun2.l.google.com:19302"
    }

  ]

};


// ------------------------------------
// MICROPHONE
// ------------------------------------

async function startIKDAMicrophone() {

  if (localStream) {
    return localStream;
  }

  localStream =
    await navigator.mediaDevices.getUserMedia({

      audio: {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true
      },

      video: false

    });

  return localStream;
}


// ------------------------------------
// CREATE PEER CONNECTION
// ------------------------------------

async function createPeerConnection(
  remoteParticipantId,
  createOffer = false
) {

  if (peers[remoteParticipantId]) {
    return peers[remoteParticipantId];
  }


  const peer =
    new RTCPeerConnection(
      rtcConfiguration
    );


  peers[remoteParticipantId] = peer;


  // Add microphone
  if (localStream) {

    localStream
      .getTracks()
      .forEach(function(track) {

        peer.addTrack(
          track,
          localStream
        );

      });

  }


  // Receive remote audio
  peer.ontrack = function(event) {

    if (!event.streams ||
        !event.streams[0]) {
      return;
    }


    let audio =
      document.getElementById(
        "audio-" + remoteParticipantId
      );


    if (!audio) {

      audio =
        document.createElement("audio");

      audio.id =
        "audio-" + remoteParticipantId;

      audio.autoplay = true;
      audio.playsInline = true;

      document.body.appendChild(audio);

    }


    audio.srcObject =
      event.streams[0];

  };


  // ICE candidate
  peer.onicecandidate =
    async function(event) {

      if (!event.candidate) {
        return;
      }


      await sendSignal({

        type: "ice",

        from: participantId,

        to: remoteParticipantId,

        candidate:
          event.candidate

      });

    };


  // Connection status
  peer.onconnectionstatechange =
    function() {

      console.log(
        "Connection:",
        remoteParticipantId,
        peer.connectionState
      );

      if (
        peer.connectionState ===
          "failed" ||

        peer.connectionState ===
          "closed"
      ) {

        closePeer(
          remoteParticipantId
        );

      }

    };


  // Create offer
  if (createOffer) {

    const offer =
      await peer.createOffer();

    await peer.setLocalDescription(
      offer
    );


    await sendSignal({

      type: "offer",

      from: participantId,

      to: remoteParticipantId,

      offer: offer

    });

  }


  return peer;
}


// ------------------------------------
// SEND SIGNAL
// ------------------------------------

async function sendSignal(message) {

  if (!roomChannel) {
    return;
  }

  try {

    await roomChannel.send({

      type: "broadcast",

      event: message.type,

      payload: message

    });

  } catch (error) {

    console.error(
      "Signal error:",
      error
    );

  }

}


// ------------------------------------
// HANDLE JOIN
// ------------------------------------

async function handleJoin(message) {

  if (
    !message ||
    message.from === participantId
  ) {
    return;
  }


  // Existing participant creates offer
  await createPeerConnection(
    message.from,
    true
  );

}


// ------------------------------------
// HANDLE OFFER
// ------------------------------------

async function handleOffer(message) {

  if (
    !message ||
    message.to !== participantId
  ) {
    return;
  }


  const peer =
    await createPeerConnection(
      message.from,
      false
    );


  await peer.setRemoteDescription(
    new RTCSessionDescription(
      message.offer
    )
  );


  const answer =
    await peer.createAnswer();


  await peer.setLocalDescription(
    answer
  );


  await sendSignal({

    type: "answer",

    from: participantId,

    to: message.from,

    answer: answer

  });

}


// ------------------------------------
// HANDLE ANSWER
// ------------------------------------

async function handleAnswer(message) {

  if (
    !message ||
    message.to !== participantId
  ) {
    return;
  }


  const peer =
    peers[message.from];


  if (!peer) {
    return;
  }


  await peer.setRemoteDescription(

    new RTCSessionDescription(
      message.answer
    )

  );

}


// ------------------------------------
// HANDLE ICE
// ------------------------------------

async function handleIce(message) {

  if (
    !message ||
    message.to !== participantId
  ) {
    return;
  }


  const peer =
    peers[message.from];


  if (!peer) {
    return;
  }


  try {

    await peer.addIceCandidate(
      new RTCIceCandidate(
        message.candidate
      )
    );

  } catch (error) {

    console.error(
      "ICE error:",
      error
    );

  }

}


// ------------------------------------
// CONNECT TO ROOM
// ------------------------------------

async function connectToIKDARoom() {

  roomChannel =
    ikdaSupabase.channel(

      "ikda-audio-room-" + roomId,

      {
        config: {
          broadcast: {
            self: false
          }
        }
      }

    );


  // New participant
  roomChannel.on(

    "broadcast",

    {
      event: "join"
    },

    function(payload) {

      handleJoin(
        payload.payload
      );

    }

  );


  // Offer
  roomChannel.on(

    "broadcast",

    {
      event: "offer"
    },

    function(payload) {

      handleOffer(
        payload.payload
      );

    }

  );


  // Answer
  roomChannel.on(

    "broadcast",

    {
      event: "answer"
    },

    function(payload) {

      handleAnswer(
        payload.payload
      );

    }

  );


  // ICE
  roomChannel.on(

    "broadcast",

    {
      event: "ice"
    },

    function(payload) {

      handleIce(
        payload.payload
      );

    }

  );


  // Subscribe
  await new Promise(
    function(resolve, reject) {

      roomChannel.subscribe(
        function(status) {

          console.log(
            "Room status:",
            status
          );


          if (
            status ===
            "SUBSCRIBED"
          ) {

            resolve();

          }


          if (
            status ===
            "CHANNEL_ERROR"
          ) {

            reject(
              new Error(
                "Unable to connect to meeting room."
              )
            );

          }

        }
      );

    }
  );


  // Tell everyone we joined
  await sendSignal({

    type: "join",

    from: participantId

  });

}


// ------------------------------------
// START LIVE MEETING
// ------------------------------------

async function startIKDALive() {

  await startIKDAMicrophone();

  await connectToIKDARoom();

}


// ------------------------------------
// CLOSE PEER
// ------------------------------------

function closePeer(
  remoteParticipantId
) {

  const peer =
    peers[remoteParticipantId];


  if (peer) {

    peer.close();

    delete peers[
      remoteParticipantId
    ];

  }


  const audio =
    document.getElementById(
      "audio-" +
      remoteParticipantId
    );


  if (audio) {
    audio.remove();
  }

}


// ------------------------------------
// LEAVE MEETING
// ------------------------------------

async function leaveIKDAMeeting() {

  Object.keys(peers)
    .forEach(function(id) {

      closePeer(id);

    });


  if (localStream) {

    localStream
      .getTracks()
      .forEach(function(track) {

        track.stop();

      });

    localStream = null;

  }


  if (roomChannel) {

    try {

      await ikdaSupabase.removeChannel(
        roomChannel
      );

    } catch (error) {

      console.error(error);

    }

    roomChannel = null;

  }

}
