# Remindly – Smart Reminder App

Remindly is a full‑stack reminder management application designed to ensure users never miss an important task. It supports real‑time notifications,time scheduling, and easy task management.

---
## 🚀 Features
- Add, edit, and delete reminders
- Real-time reminder notifications
- Notification system powered by **Django Channels + Celery + Celery Beat**
- Responsive UI using React + Tailwind CSS
- Backend powered by Django + GraphQL
- Apollo Client integrated for GraphQL communication

---
## 🛠️ Tech Stack
### **Frontend**
- React JS
- Tailwind CSS
- AppolloClient

### **Backend**
- Django
- Graphene GraphQL
- Django Admin
- MongoDB

### **Other Tools**
- JWT Authentication
- Docker (optional)

---
## 📌 Installation & Setup

Below are the simplified steps to run the Remindly application using **Docker** for the entire backend stack (Django, Channels, Celery, Celery Beat, Redis, RabbitMQ, MongoDb) and **npm start** for the frontend.

---
## 🟦 0. Prerequisites
Make sure you have the following installed:
- Docker & Docker Compose
- Node.js 18+

---
## 🟩 1. Clone the Repository
```bash
git clone https://github.com/AkshayaStephen125/Project-Remindly.git
cd app
cd remindly_backend
```

---
## 🟧 2. Start Backend (Django + Channels + Celery + Redis + RabbitMQ)
Simply run:
```bash
docker-compose up --build
```
This will automatically start:
- Django backend
- Django Channels
- Redis (channel layer)
- RabbitMQ (Celery broker)
- Celery Worker
- Celery Beat
- MongoDB

Backend will be available at → **http://localhost:8000**

---
## 🟨 3. RabbitMQ Login
Access RabbitMQ at:
- **URL:** http://localhost:15672
- **Username:** `guest`
- **Password:** `guest`

---
## 🟫 4. Start Frontend (React + Apollo Client)
```bash
cd app
cd remindly_frontend
npm install
npm start
```
Frontend runs at → **http://localhost:3000** 


---
## 🔐 GraphQL Authentication (TokenAuth)
Use the following mutation in GraphQL Playground or Postman:
```graphql
mutation TokenAuth($username: String!, $password: String!) {
  tokenAuth(username: $username, password: $password) {
    token
    payload
    refreshExpiresIn
  }
}
```
**Variables:**
```json
{
  "username": "<username>",
  "password": "<password>"
}

```

## 🤖 API Endpoints (GraphQL)
- `/graphql/` → All queries & mutations
- Authentication using TokenAuth

---

## 📄 License

This project is for personal use only. All rights reserved.

---

## 👤 Author

**Akshaya Stephen**
Built with ❤️ using Data Engineering + Power BI

