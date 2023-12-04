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

let curr = {}
let timeoutMap = {}

function listenToAgent() {

    const socket = new WebSocket('ws://127.0.0.1:8080');

    socket.addEventListener('open', (event) => {
        console.log('WebSocket connection opened:', event);
    });

    socket.addEventListener('message', (event) => {
        const resp = JSON.parse(event.data);
        console.log(resp);

        parsePubData(resp);
    });

    socket.addEventListener('close', (event) => {
        console.log('WebSocket connection closed:', event);
    });
}

function parsePubData(data) {
    curr[data.PubNum] = data;

    const currTime = currentTimeInSeconds();
    timeoutMap[data.PubNum] = currTime;

    updateUI();
    
    timeoutDelete(data.PubNum, currTime)
}

async function timeoutDelete(pubNum, time) {
    await sleep(10000);
    if (timeoutMap[pubNum] == time) {
        delete timeoutMap[pubNum];
        delete curr[pubNum];
        updateUI();
    }
}

function updateUI() {
    const container = document.getElementById('container');
    container.innerHTML = '';

    for (const key in curr) {
        const currData = curr[key];

        // Create a div element with the "object" class
        const objectDiv = document.createElement('div');
        objectDiv.className = 'object-container';

        // Create an h2 element for the title
        const titleElement = document.createElement('h2');
        titleElement.textContent = currData.Data.title;

        // Create a p element for the description
        const descriptionElement = document.createElement('p');
        descriptionElement.textContent = currData.Data.desc;

        // Create a button element with an onclick event
        const tempElement = document.createElement('p');
        tempElement.textContent = 'Temperature: ' + currData.Data.temperature;


        // Append the elements to the object div
        objectDiv.appendChild(titleElement);
        objectDiv.appendChild(descriptionElement);
        objectDiv.appendChild(tempElement);

        // Append the object div to the output div
        container.appendChild(objectDiv);
    }
}

function currentTimeInSeconds() {
    return Math.floor(Date.now() / 1000);
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function handleButtonClick(objectNum) {
    alert(`Button clicked! ${objectNum}`);
    // Add more logic or actions here
}