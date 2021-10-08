#!/usr/bin/bash
ssh -fN -L 7474:localhost:7474 -L 7687:localhost:7687 -L 3306:localhost:3306 dif03.idi.ntnu.no
