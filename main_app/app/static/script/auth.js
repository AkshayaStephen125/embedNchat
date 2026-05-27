function getCookie(name) {
    return document.cookie
        .split("; ")
        .find(row => row.startsWith(name + "="))
        ?.split("=")[1];
}

let sessionTimer = null;

function startSessionWatcher() {
    if (sessionTimer) {
        clearTimeout(sessionTimer);
    }

    const expiry = getCookie("access_expiry");
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

    console.log("Token expires in:", timeLeft / 1000, "seconds");

    if (timeLeft <= 0) {
        showSessionDialog();
    } else {
        timeLeft = timeLeft - 3000;
        if (timeLeft < 0) timeLeft = 0;

        sessionTimer = setTimeout(showSessionDialog, timeLeft);
    }
}

function showSessionDialog() {
    console.log("Showing session dialog");

    const confirmRefresh = confirm("Session expired. Continue?");

    if (!confirmRefresh) {
        window.location.href = "/signin";
        return;
    }

    refreshToken();
}

async function refreshToken() {
    const res = await fetch("/refresh", {
        method: "POST",
        credentials: "include"
    });

    if (res.ok) {
        console.log("Token refreshed");

        startSessionWatcher();
    } else {
        window.location.href = "/signin";
    }
}

window.onload = () => {
    startSessionWatcher();
};