/**
 * @class EventHandler
 * @description A utility class for managing DOM event listeners,
 * providing methods for basic, one-time, and delegated events,
 * as well as listener grouping and cleanup.
 */
class EventHandler {
  /**
   * @private
   * @type {Array<Object>}
   * @property {EventTarget} target - The DOM element the listener is attached to.
   * @property {string} event - The event type (e.g., 'click', 'mouseover').
   * @property {Function} handler - The function executed when the event fires.
   * @property {Object} options - The options object passed to addEventListener.
   * @property {?string} group - An optional name for listener grouping.
   * A private array to store all registered listeners for later removal.
   */
  constructor() {
    this.listeners = [];
  }

  /**
   * Registers a basic event listener on a target element.
   * @param {EventTarget} target - The DOM element or target to attach the listener to.
   * @param {string} event - The name of the event (e.g., 'click').
   * @param {Function} handler - The function to execute when the event is triggered.
   * @param {Object} [options={}] - Options passed to addEventListener (e.g., { capture: true }).
   * @param {?string} [group=null] - An optional group name for bulk removal.
   */
  on(target, event, handler, options = {}, group = null) {
    target.addEventListener(event, handler, options);
    this.listeners.push({ target, event, handler, options, group });
  }

  /**
   * Registers a listener that will execute only once and then automatically remove itself.
   * @param {EventTarget} target - The DOM element or target to attach the listener to.
   * @param {string} event - The name of the event.
   * @param {Function} handler - The function to execute once.
   * @param {?string} [group=null] - An optional group name for bulk removal.
   */
  once(target, event, handler, group = null) {
    const onceHandler = (e) => {
      handler(e);
      // Explicitly call off even though {once: true} is used, 
      // to ensure removal from the internal listeners array.
      this.off(target, event, onceHandler);
    };
    this.on(target, event, onceHandler, { once: true }, group);
  }

  /**
   * Removes a specific event listener.
   * @param {EventTarget} target - The target element the listener is attached to.
   * @param {string} event - The name of the event.
   * @param {Function} handler - The exact handler function to remove.
   */
  off(target, event, handler) {
    target.removeEventListener(event, handler);
    this.listeners = this.listeners.filter(
      l => !(l.target === target && l.event === event && l.handler === handler)
    );
  }

  /**
   * Registers a delegated event listener. The handler is only executed if the event
   * target matches the provided CSS selector.
   * @param {EventTarget} container - The element to attach the listener to (the delegation root).
   * @param {string} event - The name of the event.
   * @param {string} selector - The CSS selector for the elements you are interested in.
   * @param {Function} handler - The function to execute when a matching element is clicked. 
   * 'this' inside the handler will be the matched element.
   * @param {?string} [group=null] - An optional group name for bulk removal.
   */
  delegate(container, event, selector, handler, group = null) {
    const delegatedHandler = (e) => {
      const match = e.target.closest(selector);
      if (match && container.contains(match)) {
        console.log(match); //
        handler.call(match, e);
      }
    };
    // The original code uses 'false' for options, which defaults to { capture: false }
    this.on(container, event, delegatedHandler, false, group); 
  }

  /**
   * Returns an object with 'on', 'once', and 'delegate' methods 
   * that automatically tag listeners with the given group name.
   * @param {string} name - The name of the event group.
   * @returns {{on: Function, once: Function, delegate: Function}} An interface for grouped listener registration.
   */
  group(name) {
    return {
      on: (...args) => this.on(...args, name),
      once: (...args) => this.once(...args, name),
      delegate: (...args) => this.delegate(...args, name)
    };
  }

  /**
   * Removes all listeners belonging to a specific group name.
   * @param {string} name - The name of the group to remove.
   */
  removeGroup(name) {
    this.listeners = this.listeners.filter(({ target, event, handler, group }) => {
      if (group === name) {
        target.removeEventListener(event, handler);
        return false;
      }
      return true;
    });
  }

  /**
   * Removes all registered event listeners from all targets.
   */
  removeAll() {
    this.listeners.forEach(({ target, event, handler, options }) => {
      target.removeEventListener(event, handler, options);
    });
    this.listeners = [];
  }

  /**
   * Logs a table of all currently registered listeners to the console.
   */
  log() {
    console.table(this.listeners.map(({ target, event, group }) => ({
      target: target.tagName || '[object]',
      event,
      group: group || 'none'
    })));
  }
}

export default new EventHandler();