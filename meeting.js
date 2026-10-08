// ======================================================
// IKDA LIVE - AUDIO MEETING ENGINE
// Supabase Realtime + WebRTC
// ======================================================

const { createClient } = supabase;

const ikdaSupabase = createClient(
  SUPABASE_URL,
  SUPABASE_KEY
);


// ------------------------------------------------------
// Meeting information
// ------------------------------------------------------

const urlParams =
  new URLSearchParams(window.location.search);

const roomId =
  urlParams.get("room") || "IKDA-DEMO";


// Give every browser a unique participant ID.
const participantId =
  crypto.randomUUID();


// Store connected participants.
const peers = {};


// Store our microphone.
let localStream = null;


// Supabase Realtime channel.
let roomChannel = null;


// ------------------------------------------------------
// WebRTC configuration
// ------------------------------------------------------

const rtcConfiguration = {

  iceServers: [

    {
      urls: "stun:stun.l.google.com:19302"
    },

    {
      urls: "stun:stun1.l.google.com:19302"
    }

  ]

};


// ------------------------------------------------------
// Start microphone
// ------------------------------------------------------

async function startIKDAMicrophone() {

  try {

    localStream =
      await navigator.mediaDevices.getUserMedia({

        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true
        },

        video: false

      });


    console.log("IKDA microphone ready.");

    return true;

  } catch (error) {

    console.error(
      "Microphone error:",
      error
    );

    alert(
      "IKDA Live needs microphone permission to join the audio meeting."
    );

    return false;

  }

}


// ------------------------------------------------------
// Create a peer connection
// ------------------------------------------------------

function createPeerConnection(
  remoteParticipantId,
  createOffer
) {

  const peer =
    new RTCPeerConnection(
      rtcConfiguration
    );


  peers[remoteParticipantId] = peer;


  // Add our microphone to the connection.
  if (localStream) {

    localStream
      .getTracks()
      .forEach(track => {

        peer.addTrack(
          track,
          localStream
        );

      });

  }


  // Receive another participant's audio.
  peer.ontrack = function(event) {

    const audioStream =
      event.streams[0];

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
      audioStream;

  };


  // Send ICE candidates through Supabase.
  peer.onicecandidate =
    function(event) {

      if (!event.candidate) {
        return;
      }


      roomChannel.send({

        type: "broadcast",

        event: "ice",

        payload: {

          from: participantId,

          to: remoteParticipantId,

          candidate:
            event.candidate

        }

      });

    };


  // Create offer for the new participant.
  if (createOffer) {

    peer.createOffer()
      .then(offer => {

        return peer.setLocalDescription(
          offer
        );

      })
      .then(() => {

        roomChannel.send({

          type: "broadcast",

          event: "offer",

          payload: {

            from: participantId,

            to: remoteParticipantId,

            offer:
              peer.localDescription

          }

        });

      })
      .catch(error => {

        console.error(
          "Offer error:",
          error
        );

      });

  }


  return peer;

}


// ------------------------------------------------------
// Handle incoming offer
// ------------------------------------------------------

async function handleOffer(payload) {

  const from =
    payload.from;


  let peer =
    peers[from];


  if (!peer) {

    peer =
      createPeerConnection(
        from,
        false
      );

  }


  await peer.setRemoteDescription(
    new RTCSessionDescription(
      payload.offer
    )
  );


  const answer =
    await peer.createAnswer();


  await peer.setLocalDescription(
    answer
  );


  await roomChannel.send({

    type: "broadcast",

    event: "answer",

    payload: {

      from: participantId,

      to: from,

      answer:
        peer.localDescription

    }

  });

}


// ------------------------------------------------------
// Handle incoming answer
// ------------------------------------------------------

async function handleAnswer(payload) {

  const peer =
    peers[payload.from];


  if (!peer) {
    return;
  }


  await peer.setRemoteDescription(

    new RTCSessionDescription(
      payload.answer
    )

  );

}


// ------------------------------------------------------
// Handle ICE candidate
// ------------------------------------------------------

async function handleIce(payload) {

  const peer =
    peers[payload.from];


  if (!peer) {
    return;
  }


  try {

    await peer.addIceCandidate(

      new RTCIceCandidate(
        payload.candidate
      )

    );

  } catch (error) {

    console.error(
      "ICE error:",
      error
    );

  }

}


// ------------------------------------------------------
// Connect to Supabase Realtime room
// ------------------------------------------------------

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


  roomChannel.on(

    "broadcast",

    {
      event: "join"
    },

    payload => {

      const participant =
        payload.payload;


      if (
        !participant ||
        participant.id === participantId
      ) {
        return;
      }


      // The existing participant creates
      // an offer for the newcomer.
      createPeerConnection(
        participant.id,
        true
      );

    }

  );


  roomChannel.on(

    "broadcast",

    {
      event: "offer"
    },

    async payload => {

      const data =
        payload.payload;


      if (
        data.to !== participantId
      ) {
        return;
      }


      await handleOffer(data);

    }

  );


  roomChannel.on(

    "broadcast",

    {
      event: "answer"
    },

    async payload => {

      const data =
        payload.payload;


      if (
        data.to !== participantId
      ) {
        return;
      }


      await handleAnswer(data);

    }

  );


  roomChannel.on(

    "broadcast",

    {
      event: "ice"
    },

    async payload => {

      const data =
        payload.payload;


      if (
        data.to !== participantId
      ) {
        return;
      }


      await handleIce(data);

    }

  );


  const status =
    await roomChannel.subscribe();


  if (status !== "SUBSCRIBED") {

    console.error(
      "Unable to connect to IKDA room:",
      status
    );

    return false;

  }


  // Announce ourselves.
  await roomChannel.send({

    type: "broadcast",

    event: "join",

    payload: {

      id:
        participantId

    }

  });


  console.log(
    "Connected to IKDA audio room:",
    roomId
  );


  return true;

}


// ------------------------------------------------------
// Leave meeting
// ------------------------------------------------------

async function leaveIKDAMeeting() {

  Object.values(peers)
    .forEach(peer => {

      try {
        peer.close();
      } catch (_) {}

    });


  if (localStream) {

    localStream
      .getTracks()
      .forEach(track => track.stop());

  }


  if (roomChannel) {

    await ikdaSupabase.removeChannel(
      roomChannel
    );

  }

}


// ------------------------------------------------------
// Start everything
// ------------------------------------------------------

async function startIKDALive() {

  const microphoneReady =
    await startIKDAMicrophone();


  if (!microphoneReady) {
    return;
  }


  const connected =
    await connectToIKDARoom();


  if (!connected) {

    alert(
      "Unable to connect to the IKDA Live room."
    );

    return;

  }

}


// Automatically start.
startIKDALive();


// Clean up when leaving.
window.addEventListener(
  "beforeunload",
  function() {

    if (localStream) {

      localStream
        .getTracks()
        .forEach(track => track.stop());

    }

  }
);
