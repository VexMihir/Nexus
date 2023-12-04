// Wait for the DOM to be ready before executing JavaScript
document.addEventListener('DOMContentLoaded', function () {
    // Your code goes here

    connectToBackendController();

    // Example: Change the text content of an element with the ID "example"
    var exampleElement = document.getElementById('example');
    if (exampleElement) {
        exampleElement.textContent = 'Hello from script.js!';
    }

    // Example: Add a click event listener to a button with the ID "myButton"
    var myButton = document.getElementById('myButton');
    if (myButton) {
        myButton.addEventListener('click', function () {
            alert('Button clicked!');
        });
    }

    // Add more code as needed for your project
});

function connectToBackendController() {
    const socket = new WebSocket('ws://localhost:3000');

    // Connection opened
    socket.addEventListener('open', (event) => {
        console.log('Connected to server');

        // Send a message to the server
        socket.send('Hello, server!');
    });

    // Listen for messages from the server
    socket.addEventListener('message', (event) => {
        console.log(`Received from server: ${event.data}`);
    });

    // Connection closed
    socket.addEventListener('close', (event) => {
        console.log('Connection closed');
    });
}

function createWebSocketServer() {
    // Create an array to store connected clients
    const clients = [];

    // Function to broadcast a message to all connected clients
    function broadcast(message) {
        clients.forEach(client => {
            client.send(message);
        });
    }

    // Create a WebSocket server-like behavior in the frontend
    const server = new WebSocket('ws://localhost:3000');

    // Event handler for when a client connects
    server.addEventListener('open', (event) => {
        console.log('Client connected');

        // Add the client to the array of connected clients
        clients.push(server);

        // Send a welcome message to the client
        server.send('Welcome to the frontend WebSocket server!');
    });

    // Event handler for messages received from clients
    server.addEventListener('message', (event) => {
        const message = event.data;
        console.log(`Received from client: ${message}`);

        // Broadcast the message to all connected clients
        broadcast(`Server received: ${message}`);
    });

    // Event handler for when a client disconnects
    server.addEventListener('close', () => {
        console.log('Client disconnected');

        // Remove the client from the array of connected clients
        const index = clients.indexOf(server);
        if (index !== -1) {
            clients.splice(index, 1);
        }
    });
}

function handleButtonClick(objectNum) {
    alert(`Button clicked! ${objectNum}`);
    // Add more logic or actions here
}