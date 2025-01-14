# utp4j - Micro Transport Protocol for Java

This library is based on [uTP][tp] where semaphores are replaced by completableFutures.


## Current Flaws
* Correct handling of RST packets 
* Closing connection while sending/reading is not yet handled good enough
* High CPU consumption
* Probably some minor bugs.


## License
utp4j is licensed under the Apache 2.0 [license]. 

[csg]: http://www.csg.uzh.ch/
[tp]: http://www.bittorrent.org/beps/bep_0029.html
[ledbat]: http://datatracker.ietf.org/wg/ledbat/charter/
[license]: http://www.apache.org/licenses/LICENSE-2.0.html
