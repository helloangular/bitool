class EventHandler {
  constructor() {
    this.listeners = [];
  }

  // Basic event listener
  on(target, event, handler, options = {}, group = null) {
    target.addEventListener(event, handler, options);
    this.listeners.push({ target, event, handler, options, group });
  }

  // One-time listener
  once(target, event, handler, group = null) {
    const onceHandler = (e) => {
      handler(e);
      this.off(target, event, onceHandler);
    };
    this.on(target, event, onceHandler, { once: true }, group);
  }

  // Remove a specific listener
  off(target, event, handler) {
    target.removeEventListener(event, handler);
    this.listeners = this.listeners.filter(
      l => !(l.target === target && l.event === event && l.handler === handler)
    );
  }

  // Delegated event listener
  delegate(container, event, selector, handler, group = null) {
    const delegatedHandler = (e) => {
      const match = e.target.closest(selector);
      if (match && container.contains(match)) {
        handler.call(match, e);
      }
    };
    this.on(container, event, delegatedHandler, false, group);
  }

  // Group events
  group(name) {
    return {
      on: (...args) => this.on(...args, name),
      once: (...args) => this.once(...args, name),
      delegate: (...args) => this.delegate(...args, name)
    };
  }

  // Remove all listeners in a group
  removeGroup(name) {
    this.listeners = this.listeners.filter(({ target, event, handler, group }) => {
      if (group === name) {
        target.removeEventListener(event, handler);
        return false;
      }
      return true;
    });
  }

  // Remove all listeners
  removeAll() {
    this.listeners.forEach(({ target, event, handler, options }) => {
      target.removeEventListener(event, handler, options);
    });
    this.listeners = [];
  }

  // Log all listeners
  log() {
    console.table(this.listeners.map(({ target, event, group }) => ({
      target: target.tagName || '[object]',
      event,
      group: group || 'none'
    })));
  }
}

const globalEventHandler = new EventHandler()
export default globalEventHandler;
