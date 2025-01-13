"""
Entry point when running alogrithm in cage.
Initialise a listener and register the event_processor from process.py
"""
from dv_utils import DefaultListener
from process import event_processor

DefaultListener(event_processor, daemon=True)