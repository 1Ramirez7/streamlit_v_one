# DES

- added depot logic
  - depot capacity: user input
  - condemn part after x-amount of cycles, x-cycles set by user
  - added fraction of time condemn part spents at depot: user input
  - added new part oder logic: lag-day is a user input
  - added event handler to handle what happens when new part arrives

Switched build_event_index function in simulation_engine.py to using heapq library. 

- This library allows the simulation to run 3 times faster
  
- before: Queried both DataFrames every simulation period
    even if there was no event. 

- Now: Events scheduled immediately upon calculation
- Priority queue maintains chronological order 
  - It is safer then what we had