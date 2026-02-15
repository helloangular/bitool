/**
 * @class StateManager
 * @description A utility class for managing application state, providing
 * mechanisms for tracking initial state, checking for modifications ("dirty" state),
 * and committing changes.
 */
export default class StateManager {
  /**
   * @private
   * @type {Object}
   * The initial, committed, or "clean" state of the application data.
   * Used as the reference point for dirty checks.
   */
  #initialState;

  /**
   * @private
   * @type {Object}
   * The mutable current working state of the application data.
   */
  #currentState;

  /**
   * Creates an instance of StateManager.
   * The initial state is deeply cloned to prevent mutations on the original object.
   * @param {Object} initialState - The starting data object for the state manager.
   */
  constructor(initialState) {
    this.#initialState = structuredClone(initialState);
    this.#currentState = structuredClone(initialState);
  }

  /**
   * @public
   * @returns {Object} A shallow copy of the current state.
   * Ensures that external modifications do not directly alter the internal state object.
   */
  get current() {
    return {...this.#currentState};
  }

  /**
   * Updates a field in the current state with support for assignment and array operations.
   * @param {string} key - The property name in the state object to update.
   * @param {*} value - The value to assign or use depending on the operation type.
   * @param {Object} [options={ type: 'assign' }] - Additional configuration for the update.
   * @param {'assign'|'push'|'pop'|'splice'|'updateObjectInArray'} [options.type='assign'] -  
   *   Operation type:
   *   - **assign**: Replace the field value with `value`.
   *   - **push**: Push `value` into the array at `key`.
   *   - **pop**: Remove the last item from the array at `key` (ignores `value`).
   *   - **splice**: Remove one element from array at `key` at `options.index`.
   *   - **updateObjectInArray**: Merge `value` into the object at array index `options.index`.
   * @param {number} [options.index] - The index used by `splice` and `updateObjectInArray`.
   */
  updateField(key, value, options = {type: "assign"}) {
    if (!key || key.trim() === "") return;
    if (!(key in this.#currentState)) return;
    const {type} = options;
    
    if (type === "assign") this.#currentState[key] = value;
    else if (type === "push") this.#currentState[key].push(value);
    else if (type === "pop") this.#currentState[key].pop(); // value is not used in Array.prototype.pop()
    else if (type === "splice" && options.index !== undefined) this.#currentState[key].splice(options.index, 1)
    else if (type === "updateObjectInArray" && options.index !== undefined) this.#currentState[key][options.index] = Object.assign(this.#currentState[key][options.index], value);
  }

  /**
   * Checks if the current state differs from the initial/committed state.
   * @returns {boolean} True if the state has been modified (is dirty), false otherwise.
   */
  isDirty() {
    return !this.deepEqual(this.#initialState, this.#currentState);
  }

  /**
   * Performs a recursive, deep comparison between two objects, with a special check
   * for arrays that compares them ignoring element order (treating them like sets).
   * @param {Object | Array} objA - The first object to compare.
   * @param {Object | Array} objB - The second object to compare.
   * @returns {boolean} True if the objects are deeply equal, false otherwise.
   */
  deepEqual(objA, objB) {
    if (objA === objB) return true; // primitives or same reference

    if (typeof objA !== 'object' || objA === null ||
      typeof objB !== 'object' || objB === null) {
      return false; // one is primitive or null
    }

    const keysA = Object.keys(objA);
    const keysB = Object.keys(objB);
    if (keysA.length !== keysB.length) return false;

    for (const key of keysA) {
      if (!keysB.includes(key)) return false;

      const valA = objA[key];
      const valB = objB[key];

      // If the value is an array, compare as sets (ignore order)
      if (Array.isArray(valA) && Array.isArray(valB)) {
        if (valA.length !== valB.length) return false;

        // Sort copies of arrays before comparing elements recursively
        // NOTE: This assumes array elements are sortable (e.g., primitives or objects that can be serialized for comparison).
        const sortedA = [...valA].sort();
        const sortedB = [...valB].sort();

        for (let i = 0; i < sortedA.length; i++) {
          if (!this.deepEqual(sortedA[i], sortedB[i])) return false;
        }
      } else {
        // normal recursive check for objects or primitives
        if (!this.deepEqual(valA, valB)) return false;
      }
    }

    return true;
  }

  get updatedFields() {
    return this.getUpdatedFields(this.#initialState, this.#currentState);
  }

  getUpdatedFieldsWithRequiredFields(feilds, nested = true) {
    return this.getUpdatedFields(this.#initialState, this.#currentState, feilds, nested);
  }

  getUpdatedFields(initial, current, feilds = null, nested = true) {
    let updatedFields = {};

    for (const [key, value] of Object.entries(current)) {
      if (initial[key] === undefined) {
        updatedFields[key] = value;
      } else if (Array.isArray(initial[key]) && Array.isArray(current[key])) {
        if (initial[key].length !== current[key].length) {
          updatedFields[key] = current[key]
        } else if (typeof initial[key][0] === 'object' && typeof current[key][0] === 'object' && nested) {
          const nestedUpdates = [];
          for (let i = 0; i < current[key].length; i++) {
            if (!this.deepEqual(initial[key][i], current[key][i])) {
              nestedUpdates.push(current[key][i]);
            }
          }
          if (nestedUpdates.length > 0) {
            updatedFields[key] = nestedUpdates;
          }
        } else {
          if (!this.arraysEqualAsSets(initial[key], current[key])) {
            updatedFields[key] = value;
          }
        }
      } else if (initial[key] instanceof Object && current[key] instanceof Object) {
        const nestedUpdates = this.getUpdatedFields(initial[key], current[key]);
        if (Object.keys(nestedUpdates).length > 0) {
          updatedFields[key] = nestedUpdates;
        }
      } else if (initial[key] !== value) {
        updatedFields[key] = value;
      } else if (feilds && feilds.includes(key)) {
        updatedFields[key] = value;
      }
    }

    return updatedFields
  }

  arraysEqualAsSets(a, b) {
    if (a.length !== b.length) return false;

    const used = new Array(b.length).fill(false);

    return a.every(itemA => {
      // find matching itemB (deep equal)
      for (let i = 0; i < b.length; i++) {
        if (!used[i] && this.deepEqual(itemA, b[i])) {
          used[i] = true;
          return true;
        }
      }
      return false;
    });
  }

  /**
   * Commits the current state to the initial state, effectively marking the state as "clean."
   */
  commit() {
    this.#initialState = structuredClone(this.#currentState);
  }
}
