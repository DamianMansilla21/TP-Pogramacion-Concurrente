#include "semaforo.h"
#include <mutex>
#include <condition_variable>

void init(Semaforo& s, int n) {
    s.contador = n;
}

void wait(Semaforo& s) {
    std::unique_lock<std::mutex> lock(s.mtx);

    while (s.contador == 0) {
        s.cv.wait(lock);  // bloquea el hilo
    }

    s.contador--;  // consume un permiso
}

void signal(Semaforo& s) {
    std::unique_lock<std::mutex> lock(s.mtx);

    s.contador++;        // libera un permiso
    s.cv.notify_one();   // despierta UN hilo en espera
}
