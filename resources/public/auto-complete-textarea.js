import EventHandler from "./library/eventHandler.js";

// Define the template for the custom element
const template = document.createElement('template');
template.innerHTML = `
  <style>
    #auto-complete-textarea{
      width: 100%;
      min-height: 60px;
      border: 1px solid #ccc;
      margin-bottom: 10px;
      padding: 8px;
    }

    #suggestions {
      box-shadow: 0 0 25px rgba(0, 0, 0, 0.05);
      width: 450px;
    }

    #suggestions>div {
      padding: 10px;
    }
  </style>
  <textarea id="auto-complete-textarea" rows="4" cols="50" placeholder="start typing here..."></textarea>
  <div id="suggestions" style="display: none;"></div>
`;

class AutoCompleteTextArea extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: 'open' });
    this.shadowRoot.appendChild(template.content.cloneNode(true));
  }

  connectedCallback() {
    this.textarea = this.shadowRoot.querySelector('#auto-complete-textarea');
    this.suggestionsDiv = this.shadowRoot.querySelector('#suggestions');
    EventHandler.on(this.textarea, 'input', this.handleInput.bind(this));
    EventHandler.on(this.textarea, 'keydown', (event) => {
      if (event.key === 'Enter') {
        event.preventDefault(); // Prevent default behavior of Enter key
        const firstSuggestion = this.suggestionsDiv.querySelector('div');
        if (firstSuggestion) {
          this.selectSuggestion(firstSuggestion.textContent);
        }
        this.textarea.focus(); // Refocus the textarea
      }
    });
    this.suggestions = [
      "+",
      "-",
      "/",
      "*",
      "(",
      ")",
      "=",
      "||",
      "AND",
      "OR",
      "NOT",
      "BETWEEN",
      "<",
      ">",
      "<=",
      ">=",
      "=",
      "!=",
      "IS NULL",
      "ANY",
      "SOME",
      "ALL",
      "CONTAINS",
      "EXISTS",
      "IN",
      "LIKE",
      "MEMBER OF",
      "NULL",
      "CASE WHEN THEN ELSE END"
    ];
  }

  getTextContent() {
    return this.textarea.value;
  }

  setTextContent(value) {
    this.textarea.value = value;
  }

  handleInput(event) {
    const inputText = event.target.value;
    if ((inputText.length == 0 && inputText.trim() === '') || inputText.endsWith(' ')) {
      this.suggestionsDiv.style.display = 'none'; // Hide suggestions if input is empty
      return;
    }
    const lastWord = inputText.split(' ').pop();
    const suggestions = this.getSuggestions(lastWord);
    this.showSuggestions(suggestions);
  }

  getSuggestions(inputText) {
    const match = inputText.match(/\(([^)]+)\)/); // "/\(([^)]*)$/" Match text after an open parenthesis
    if (match) {
      const query = match[1]; // Extract the text after the open parenthesis
      return this.suggestions.filter(item => item.startsWith(query.toUpperCase()));
    }
    return this.suggestions.filter(item => item.startsWith(inputText.toUpperCase()));
  }

  showSuggestions(suggestions) {
    this.suggestionsDiv.innerHTML = '';
    if (suggestions.length > 0) {
      suggestions.forEach(suggestion => {
        const suggestionItem = document.createElement('div');
        suggestionItem.textContent = suggestion;
        EventHandler.on(suggestionItem, 'click', () => this.selectSuggestion(suggestion));
        this.suggestionsDiv.appendChild(suggestionItem);
      });
      this.suggestionsDiv.style.display = 'block';
    } else {
      this.suggestionsDiv.style.display = 'none';
    }
  }

  selectSuggestion(suggestion) {
    const inputText = this.textarea.value.split(' ');
    const lastWord = inputText.pop();
    const match = lastWord.match(/\(([^)]+)\)/); // Match text after an open parenthesis

    if (match) {
      // If the last word contains an open parenthesis, separate the suggestion
      const prefix = lastWord.slice(0, match.index + 1); // Keep the text before the parenthesis
      const suffix = lastWord.slice(match.index + match[1].length + 1); // Keep the text after the closing parenthesis
      inputText.push(prefix + suggestion + suffix); // Append the suggestion with the prefix and suffix
    } else {
      inputText.push(suggestion); // Add the selected suggestion
    }

    this.textarea.value = inputText.join(' ') + ' ';
    this.suggestionsDiv.style.display = 'none'; // Hide suggestions after selection
  }
}

// Define the custom element
customElements.define('auto-complete-textarea', AutoCompleteTextArea);