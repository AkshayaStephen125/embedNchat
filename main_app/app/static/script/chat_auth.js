let sessionTimer = null;
const BASE_URL = "http://localhost:8000";

const WS_URL = BASE_URL.replace("http", "ws") + "/ws/refresh";

function startSessionWatcher() {
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

    console.log("Token refreshes in:", timeLeft / 1000, "seconds");

    if (timeLeft <= 0) {
        showSessionDialog();
    } else {
        timeLeft = timeLeft - 3000;
        if (timeLeft < 0) timeLeft = 0;

        sessionTimer = setTimeout(refreshToken, timeLeft);
    }
}


// ✅ Refresh token
async function refreshToken() {
    await fetch(WS_URL, {
        method: "POST",
        credentials: "include",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({"refresh_token": localStorage.getItem("refresh_token")})
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
window.onload = () => {
    startSessionWatcher();
};