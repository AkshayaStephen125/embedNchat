(async function () {
    let sessionTimer = null;


    const scriptTag = document.querySelector('script[data-api-key]');
    const API_KEY = scriptTag?.getAttribute("data-api-key");
    const BASE_URL = "http://localhost:8000";
    const CONFIG_URL = BASE_URL + "/api/config";
    const INIT_TOKEN_URL = BASE_URL + "/api/init-token";
    const REFRESH_TOKEN_URL = BASE_URL + "/api/refresh-token";
    const WS_URL = BASE_URL.replace("http", "ws") + "/ws/chat";

    let ws = null;
    let lastEventName = null;

    if (!API_KEY) {
        console.error("TenantAI: Missing API key");
        return;
    }

    async function loadWidgetConfig() {
        try {
            const res = await fetch(CONFIG_URL, {
                method: "POST",
                headers: {
                    "x-api-key": API_KEY
                }
            });

            if (!res.ok) throw new Error("Failed to load config");
            return await res.json();

        } catch (err) {
            console.error("Theme load failed:", err);
            return null;
        }
    }

    const config = await loadWidgetConfig();

    // ---------- Theme Variables ----------
    const LOGO = BASE_URL+config?.theme?.logo || "https://cdn-icons-png.flaticon.com/512/4712/4712027.png";
    const BRAND_NAME = config?.theme?.brand_name || "TenantAI ";
    const WELCOME_MESSAGE = config?.theme?.welcome_message || "Hi, How can I assist you?";
    const HEADER_COLOR = config?.theme?.header_color || "#4f46e5";
    const BACKGROUND_COLOR = config?.theme?.background_color || "#f9fafb";
    const USER_MSG_COLOR = config?.theme?.user_message_color || HEADER_COLOR;
    const BOT_MSG_COLOR = config?.theme?.bot_message_color || "#ffffff";

    // ---------- Animations ----------
    const style = document.createElement("style");
    style.innerHTML = `
        @keyframes fadeIn {
            from {opacity:0; transform:translateY(8px);}
            to {opacity:1; transform:translateY(0);}
        }

        @keyframes blink {
        0% { opacity: .2; }
        20% { opacity: 1; }
        100% { opacity: .2; }
        }
        .typing-dot {
        width: 6px;
        height: 6px;
        margin: 0 2px;
        background: #999;
        border-radius: 50%;
        display: inline-block;
        animation: blink 1.4s infinite both;
        }
        .typing-dot:nth-child(2) { animation-delay: .2s; }
        .typing-dot:nth-child(3) { animation-delay: .4s; }
    `;
    document.head.appendChild(style);

    // ---------- Launcher ----------
    const launcher = document.createElement("div");
    launcher.innerHTML = `
  <svg width="800px" height="800px" viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg">

    <g fill="none" fill-rule="evenodd">

    <circle cx="16" cy="16" r="16" fill="${HEADER_COLOR}"/>

    <path fill="#FFF" d="M16.28 23.325a11.45 11.45 0 002.084-.34 5.696 5.696 0 002.602.17.627.627 0 01.104-.008c.31 0 .717.18 1.31.56v-.625a.61.61 0 01.311-.531c.258-.146.498-.314.717-.499.864-.732 1.352-1.708 1.352-2.742 0-.347-.055-.684-.159-1.006.261-.487.472-.999.627-1.53A4.59 4.59 0 0126 19.31c0 1.405-.654 2.715-1.785 3.673a5.843 5.843 0 01-.595.442v1.461c0 .503-.58.792-.989.493a15.032 15.032 0 00-1.2-.81 2.986 2.986 0 00-.368-.187c-.34.051-.688.077-1.039.077-1.412 0-2.716-.423-3.743-1.134zm-7.466-2.922C7.03 18.89 6 16.829 6 14.62c0-4.513 4.258-8.12 9.457-8.12 5.2 0 9.458 3.607 9.458 8.12 0 4.514-4.259 8.121-9.458 8.121-.584 0-1.162-.045-1.728-.135-.245.058-1.224.64-2.635 1.67-.511.374-1.236.013-1.236-.616v-2.492a9.27 9.27 0 01-1.044-.765zm4.949.666c.043 0 .087.003.13.01.51.086 1.034.13 1.564.13 4.392 0 7.907-2.978 7.907-6.589 0-3.61-3.515-6.588-7.907-6.588-4.39 0-7.907 2.978-7.907 6.588 0 1.746.821 3.39 2.273 4.62.365.308.766.588 1.196.832.241.136.39.39.39.664v1.437c1.116-.749 1.85-1.104 2.354-1.104zm-2.337-4.916c-.685 0-1.24-.55-1.24-1.226 0-.677.555-1.226 1.24-1.226.685 0 1.24.549 1.24 1.226 0 .677-.555 1.226-1.24 1.226zm4.031 0c-.685 0-1.24-.55-1.24-1.226 0-.677.555-1.226 1.24-1.226.685 0 1.24.549 1.24 1.226 0 .677-.555 1.226-1.24 1.226zm4.031 0c-.685 0-1.24-.55-1.24-1.226 0-.677.555-1.226 1.24-1.226.685 0 1.24.549 1.24 1.226 0 .677-.555 1.226-1.24 1.226z"/>

    </g>

    </svg>
    `;

    Object.assign(launcher.style, {
        position: "fixed",
        bottom: "24px",
        right: "24px",
        width: "64px",
        height: "64px",
        borderRadius: "50%",
        background: HEADER_COLOR,
        color: "#fff",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        cursor: "pointer",
        boxShadow: "0 12px 30px rgba(0,0,0,.25)",
        zIndex: "999999",
        transition: "transform .2s ease"
    });

    launcher.onmouseenter = () => launcher.style.transform = "scale(1.1)";
    launcher.onmouseleave = () => launcher.style.transform = "scale(1)";
    document.body.appendChild(launcher);

    // ---------- Chat Container ----------
    const chat = document.createElement("div");
    Object.assign(chat.style, {
        position: "fixed",
        bottom: "100px",
        right: "24px",
        width: "360px",
        height: "520px",
        background: BACKGROUND_COLOR,
        borderRadius: "16px",
        boxShadow: "0 25px 60px rgba(0,0,0,.2)",
        display: "none",
        flexDirection: "column",
        overflow: "hidden",
        fontFamily: "Inter, system-ui, sans-serif",
        zIndex: "999999"
    });
    document.body.appendChild(chat);

    // ---------- Header ----------
    const header = document.createElement("div");
    Object.assign(header.style, {
        background: HEADER_COLOR,
        color: "#fff",
        padding: "14px 16px",
        display: "flex",
        alignItems: "center",
        gap: "10px"
    });

    const logo = document.createElement("img");
    logo.src = LOGO;
    Object.assign(logo.style, {
        width: "34px",
        height: "34px",
        borderRadius: "8px",
        background: "#fff",
        padding: "4px"
    });

    const title = document.createElement("div");
    title.innerText = BRAND_NAME;
    title.style.fontWeight = "600";

    header.appendChild(logo);
    header.appendChild(title);
    chat.appendChild(header);

    // ---------- Messages ----------
    const messages = document.createElement("div");
    Object.assign(messages.style, {
        flex: "1",
        padding: "16px",
        overflowY: "auto",
        background: BACKGROUND_COLOR
    });
    chat.appendChild(messages);

    // ---------- Input ----------
    const inputBar = document.createElement("div");
    Object.assign(inputBar.style, {
        display: "flex",
        borderTop: "1px solid #eee",
        padding: "10px",
        background: "#fff"
    });

    const input = document.createElement("input");
    input.placeholder = "Write a message...";
    input.style.flex = "1";
    input.style.border = "none";
    input.style.outline = "none";

    const send = document.createElement("button");
    send.innerHTML = "➤";
    Object.assign(send.style, {
        border: "none",
        background: USER_MSG_COLOR,
        color: "#fff",
        width: "38px",
        height: "38px",
        borderRadius: "50%",
        cursor: "pointer"
    });

    inputBar.appendChild(input);
    inputBar.appendChild(send);
    chat.appendChild(inputBar);

    // ---------- Toggle ----------
    launcher.onclick = () => {
        chat.style.display = chat.style.display === "none" ? "flex" : "none";
    };

    function mdToText(str) {
    return str
        .replace(/#+\s/g, '')               // remove headers
        .replace(/\*\*(.*?)\*\*/g, '$1')    // bold
        .replace(/\*(.*?)\*/g, '$1')        // italic
        .replace(/`(.*?)`/g, '$1')          // inline code
        .replace(/^(\s*[-*]\s)/gm, '• ')    // unordered lists
        .replace(/^\s*\d+\.\s/gm, '• ')     // ordered lists
        .trim();
}

    function showTyping() {
    const row = document.createElement("div");
    row.style.display = "flex";
    row.style.marginBottom = "10px";
    row.style.justifyContent = "flex-start";
    row.id = "typing-indicator";

    const bubble = document.createElement("div");

    Object.assign(bubble.style, {
        padding: "10px 14px",
        borderRadius: "16px",
        background: BOT_MSG_COLOR,
        border: "1px solid #e5e7eb"
    });

    bubble.innerHTML = `
        <span class="typing-dot"></span>
        <span class="typing-dot"></span>
        <span class="typing-dot"></span>
    `;

    row.appendChild(bubble);
    messages.appendChild(row);
    messages.scrollTop = messages.scrollHeight;
}

function removeTyping() {
    const typing = document.getElementById("typing-indicator");
    if (typing) typing.remove();
}

        async function getSessionToken() {

        let chatToken = localStorage.getItem("chat_token");
        let refreshToken = localStorage.getItem("refresh_chat_token");
        let accessExpiry = localStorage.getItem("access_expiry");


        console.log('chatToken      ',chatToken)
        console.log('refresh_chat_token      ',refreshToken)
        console.log('access_expiry      ',accessExpiry)



        if (!chatToken) {
           
        try {
            const res = await fetch(INIT_TOKEN_URL, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-API-KEY": API_KEY
            },
            credentials: "include"
            });

            if (!res.ok) throw new Error("Failed to get token");
            const tokenData = await res.json();
            chatToken = tokenData.token;
            refreshToken = tokenData.refresh_token
            accessExpiry = tokenData.access_expiry
            console.log("tokenData  ",tokenData)
            localStorage.setItem("chat_token", chatToken);
            localStorage.setItem("refresh_chat_token", refreshToken)
            localStorage.setItem("access_expiry", accessExpiry)


        } catch (err) {
            console.error("Failed to get token", err);
            return null;
        }
    }
    return chatToken
    }

    // ---------- Add Message ----------
    function addMessage(text, type) {

        const row = document.createElement("div");
        row.style.display = "flex";
        row.style.marginBottom = "10px";

        const bubble = document.createElement("div");
        bubble.innerText = mdToText(text);

        Object.assign(bubble.style, {
            padding: "10px 14px",
            borderRadius: "16px",
            maxWidth: "80%",
            wordWrap: "break-word",
            animation: "fadeIn .2s ease"
        });

        if (type === "User") {
            row.style.justifyContent = "flex-end";
            bubble.style.background = USER_MSG_COLOR;
            bubble.style.color = "#fff";
        } 
        else if(type === "System"){
            const info = document.createElement("div");
            info.innerText = text;

            row.style.display = "flex";
            row.style.justifyContent = "center";
            row.style.margin = "8px 0";

            info.style.fontSize = "12px";
            info.style.color = "#888";

            row.appendChild(info);
            messages.appendChild(row);
            messages.scrollTop = messages.scrollHeight;
            return;
        }
        else {
            row.style.justifyContent = "flex-start";
            bubble.style.background = BOT_MSG_COLOR;
            bubble.style.border = "1px solid #e5e7eb";
        }

        row.appendChild(bubble);
        messages.appendChild(row);
        messages.scrollTop = messages.scrollHeight;
    }

    addMessage(WELCOME_MESSAGE, "bot");

    const token = await getSessionToken();
        
        ws = new WebSocket(WS_URL+"?token="+token);

        ws.onopen = async function() {
            console.log("WebSocket connection established");
        };

        ws.onmessage = function(event) {
            removeTyping();
            const data = JSON.parse(event.data);
            lastEventName = data.event
            addMessage(data.message, data.sender);
            console.log("lastEventName  ",lastEventName)
            
        }

        // ws.onerror = function (err) {
    //     console.error("WebSocket error", err);
    // };

    async function sendMessage() {

          if (!ws || ws.readyState !== WebSocket.OPEN) {
        console.error("WebSocket not open. Current state:", ws?.readyState);
        removeTyping();
        addMessage("Connection lost. Reconnecting...", "bot");

        // initWebSocket();  // auto reconnect
        // return;
    }


        const text = input.value.trim();
        if (!text) return;

        addMessage(text, "User");
        input.value = "";
        showTyping();

        try {
            ws.send(JSON.stringify({ message: text, sender: "USER", event: lastEventName || "BOT_MESSAGE" }))

        } catch(error) {
            removeTyping();
            addMessage("Unable to reach server. "+error, "bot");
        }
    }

    send.onclick = sendMessage;
    input.addEventListener("keypress", e => {
        if (e.key === "Enter") sendMessage();
    });

    async function token_lookup() {
        
    }

    // }
    // initWebSocket()



function startSessionWatcher() {
    console.log("Activatedd")
    if (sessionTimer) {
        clearTimeout(sessionTimer);
    }
    const expiry = localStorage.getItem("access_expiry");
    console.log("expiry ",expiry)
    const date = new Date(expiry * 1000);

    console.log("timee      ",date.toString());
    

    if (!expiry) {
        console.log("No expiry found");
        return;
    }

    const expiryTime = parseInt(expiry) * 1000;
    const now = Date.now();

    let timeLeft = expiryTime - now;
    console.log("timeLeft   ",timeLeft)

    console.log("Token refreshes in:", timeLeft / 1000, "seconds");

    if (timeLeft <= 0) {
        refreshToken();
    } else {
        timeLeft = timeLeft - 3000;
        if (timeLeft < 0) timeLeft = 0;

        sessionTimer = setTimeout(refreshToken, timeLeft);
    }
}


// ✅ Refresh token
async function refreshToken() {
    await fetch(REFRESH_TOKEN_URL, {
        method: "POST",
        credentials: "include",
        headers: {
            "x-api-key": API_KEY,
            "Content-Type": "application/json"
        },
        body: JSON.stringify({"refresh_token": localStorage.getItem("refresh_chat_token")})
    })
    .then(res => res.json())
    .then(data => {
        if (data.result) {
            console.log("Token refreshed");
        localStorage.setItem("access_expiry", data.access_expiry)

        // restart timer
        startSessionWatcher();

        }
        else{
            console.log("Token refresh failed");
        }
    })
    .catch(err => console.error(err));
}



// ✅ Start automatically
// window.onload = () => {
    startSessionWatcher();
// };
})();
