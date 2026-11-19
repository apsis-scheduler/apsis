import Vue from 'vue'

Vue.config.productionTip = false

// jsdom has no text selection
window.getSelection = () => ({isCollapsed: true})
