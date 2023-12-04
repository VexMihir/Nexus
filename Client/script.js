// Wait for the DOM to be ready before executing JavaScript
document.addEventListener('DOMContentLoaded', function () {
    // Your code goes here

    connectToBackendController();
    listenToAgent();

    // Example: Change the text content of an element with the ID "example"
    var exampleElement = document.getElementById('example');
    if (exampleElement) {
        exampleElement.textContent = 'Hello from script.js!';
    }

    // Example: Add a click event listener to a button with the ID "myButton"
    // var myButton = document.getElementById('myButton');
    // if (myButton) {
    //     myButton.addEventListener('click', function () {
    //         alert('Button clicked!');
    //     });
    // }

    // Add more code as needed for your project
});

function connectToBackendController() {
    const socket = new WebSocket('ws://127.0.0.1:12345');

    // Connection opened
    socket.addEventListener('open', (event) => {
        console.log('Connected to server');

        const obj = { "action": "subscribe", "subscriptions": ["Temperature"], "listener-address": "127.0.0.1:8080" };
        // Send a message to the server
        socket.send(JSON.stringify(obj));
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

function listenToAgent() {

    const socket = new WebSocket('ws://127.0.0.1:8080');
    
    socket.addEventListener('open', (event) => {
        console.log('WebSocket connection opened:', event);
    });

    socket.addEventListener('message', (event) => {
        const messagesList = document.getElementById('messages');
        console.log(messagesList);
        const li = document.createElement('li');
        li.textContent = event.data;
        messagesList.appendChild(li);
    });

    socket.addEventListener('close', (event) => {
        console.log('WebSocket connection closed:', event);
    });
}

function handleButtonClick(objectNum) {
    alert(`Button clicked! ${objectNum}`);
    // Add more logic or actions here
}