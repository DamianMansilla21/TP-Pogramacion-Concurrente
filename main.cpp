#include <iostream>
#include <thread>
#include <mutex>
#include <chrono>
#include <queue>

#include "semaforo.h"

using namespace std;

struct Paquete {
    int id;
    int prioridad;
    std::chrono::system_clock::time_point fechaDeCreacion;
};

std::queue<Paquete> prioridadAlta;//estanteria
std::queue<Paquete> prioridadBaja;
std::queue<Paquete> buffer2;//cinta

std::mutex mtxBuffer1;
std::mutex mtxBuffer2;
std::mutex mtxConsola;

int pedido = 20;

int paquetesPendientes = pedido;
int siguienteId = 1;
int contadorAltas = 0;

Semaforo hayDatosBuffer1; //waitingQueue
Semaforo hayDatosBuffer2; //Processing Queue
Semaforo HayEspacioBuffer2; //Processing Queue


void productor() //Guardamos los paquetes en el buffer 1
{
    while(pedido>0)
    {
        Paquete p;

        p.id = siguienteId++;
        p.prioridad = rand() % 2;
        p.fechaDeCreacion = chrono::system_clock::now(); //Guardamos la fecha de hoy

        //wait(HayEspacioBuffer1);

        if(p.prioridad == 1){

            mtxBuffer1.lock();
            prioridadAlta.push(p);

            mtxConsola.lock();
            std::cout << "paquete creado: ";
            cout << "ID: " << p.id << " Prioridad: " << p.prioridad << endl;
            mtxConsola.unlock();

            mtxBuffer1.unlock();

         } else {

            mtxBuffer1.lock();
            prioridadBaja.push(p);

            mtxConsola.lock();
            std::cout << "paquete creado: ";
            cout << "ID: " << p.id << " Prioridad: " << p.prioridad << endl;
            mtxConsola.unlock();

            mtxBuffer1.unlock();
         }


        signal(hayDatosBuffer1);

        this_thread::sleep_for(chrono::milliseconds(90));

        pedido--;

    }
}

void consumidor() //Saca los paquetes del buffer 2
{
    while(paquetesPendientes > 0)
    {
        wait(hayDatosBuffer2);

        Paquete p;

        mtxBuffer2.lock();
        p = buffer2.front(); //primer elementos
        buffer2.pop();
        mtxBuffer2.unlock();

        signal(HayEspacioBuffer2);

        mtxConsola.lock();

        time_t t = chrono::system_clock::to_time_t(p.fechaDeCreacion);

        cout << "ID: " << p.id << " Prioridad: " << p.prioridad << " Fecha: " << ctime(&t); // lo pasa a un texto legible

        mtxConsola.unlock();

        paquetesPendientes--;
    }

}


void cargarEnCinta(){

    while(paquetesPendientes > 0){

        wait(hayDatosBuffer1);
        wait(HayEspacioBuffer2);

        std::this_thread::sleep_for(std::chrono::milliseconds(420));

        if(prioridadAlta.size()>0 && contadorAltas < 3){

            Paquete p = prioridadAlta.front();
            prioridadAlta.pop();

            buffer2.push(p);

            mtxConsola.lock();
            std::cout << "paquete en cinta: ";
            cout << "ID: " << p.id << " Prioridad: " << p.prioridad << endl;
            mtxConsola.unlock();

            contadorAltas++;
        } else if(prioridadBaja.size()>0){

            Paquete p = prioridadBaja.front();
            prioridadBaja.pop();

            buffer2.push(p);

            mtxConsola.lock();
            std::cout << "paquete en cinta: ";
            cout << "ID: " << p.id << " Prioridad: " << p.prioridad << endl;
            mtxConsola.unlock();

            contadorAltas = 0;
        } else if(prioridadAlta.size()>0 ){

            Paquete p = prioridadAlta.front();
            prioridadAlta.pop();

            buffer2.push(p);

            mtxConsola.lock();
            std::cout << " paquete en cinta: ";
            cout << "ID: " << p.id << " Prioridad: " << p.prioridad << endl;
            mtxConsola.unlock();

            contadorAltas++;
        }

        signal(hayDatosBuffer2);
    }

}


int main()
{
    init(HayEspacioBuffer2,5);
    init(hayDatosBuffer1,0);
    init(hayDatosBuffer2,0);

    thread p1(productor);
    thread c1(cargarEnCinta);
    thread r1(consumidor);

    p1.join();
    c1.join();
    r1.join();


}

//prioridad aleatoria
//fecha de creacion
//ubicar lock y unlock
//unir crearPaquete y cargarEnCintan en una sola funcion
//retardos simulados
