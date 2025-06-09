
#include "Globals.h"
Globals::Globals() {
    for(int i=0; i<UserInput::getTranClients(); i++){
        containers.push_back(new LinkedList());
    }
}

int Globals::getLoOrderKey() {
    return ++loOrderKey;
}

time_t Globals::GetEpochTime(){
    return epoch_time;
}
