# Custom MP functions

This repo will host custom multiprocessing functions/classes for personal use.

---
## Installation

With pip from github:
```
pip install git+https://github.com/mathewhuen/customp.git
```

With pip locally:
```
# clone repo
git clone https://github.com/mathewhuen/customp.git
pip install ./customp
```

---

## API

#### bmap
A more balanced approach to io/cpu intensive applications. bmap accepts three
functions: an io-focused pre function, a cpu-focused main function, and an
io-focused post function. The development goal for this function is to better
support a file load -> file process -> file save workflow using both
multiprocessing and multithreading in a general way.

