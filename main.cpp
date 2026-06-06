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
    std::chrono::system_clock::time_point ingresoProcessing; //momento en el que ingresa a la cinta
};

std::vector<Paquete> buffer1;//estanteria
std::queue<Paquete> buffer2;//cinta

std::mutex mtxBuffer1;
std::mutex mtxBuffer2;
std::mutex mtxConsola;
std::mutex mtxPedido;
std::mutex mtxPendientes;
std::mutex mtxId;

int pedido = 30;
int nivelPrioridad = 5;

int paquetesPendientes = pedido;
int siguienteId = 1;
int contadorAltas = 0;
int marcador = 0;
int totalProducidos = 0;

int SumAlta = 0; //suma el total de tiempo de espera de todos los paquetes con prioridad alta
int SumBaja = 0; //suma el total de tiempo de espera de todos los paquetes con prioridad baja
int CantAlta = 0; //la cantidad de paquetes de prioridad alta
int cantBaja = 0; //la cantidad de paquetes de prioridad baja

Semaforo hayDatosBuffer1; //waitingQueue
//Semaforo hayDatosBuffer2; //Processing Queue
Semaforo HayEspacioBuffer2; //Processing Queue


void productor() //Guardamos los paquetes en el buffer 1
{
    while(pedido>0){
        mtxPedido.lock();
        if(pedido > 0){
            pedido--;
            mtxPedido.unlock();
        } else {
            mtxPedido.unlock();
            break;
        }

        Paquete p;

        mtxId.lock();
        p.id = siguienteId++;
        mtxId.unlock();

        p.prioridad = rand() % 2;
        p.fechaDeCreacion = chrono::system_clock::now(); //Guardamos la fecha de hoy

        //wait(HayEspacioBuffer1);

        mtxBuffer1.lock();

        if(p.prioridad == 1){
            buffer1.insert(buffer1.begin() + marcador, p);
            marcador++;
        }else{
            buffer1.push_back(p);
        }

        totalProducidos++;

        mtxConsola.lock();
        std::cout << "paquete creado: ";
        std::cout << "ID: " << p.id << " Prioridad: " << p.prioridad << std::endl;
        mtxConsola.unlock();

        mtxBuffer1.unlock();


        signal(hayDatosBuffer1);

        this_thread::sleep_for(chrono::milliseconds(90));

    }
}

void consumidor()
{
    while(paquetesPendientes > 0)
    {
        // Protegemos la lectura de paquetesPendientes dentro del while
        mtxPendientes.lock();
        if(paquetesPendientes <= 0) {
            mtxPendientes.unlock();
            break;
        }
        mtxPendientes.unlock();

        wait(hayDatosBuffer1);
        wait(HayEspacioBuffer2);

        std::this_thread::sleep_for(std::chrono::milliseconds(420));

        Paquete p;

        // Sacar de la Waiting Queue según prioridad
        mtxBuffer1.lock();
        if(contadorAltas < nivelPrioridad && marcador > 0)
        {
            p = buffer1.front();

            buffer1.erase(buffer1.begin());

            marcador--;
            contadorAltas++;
        }
        else if(marcador < buffer1.size())
        {
            p = buffer1[marcador];

            buffer1.erase(buffer1.begin() + marcador);

            contadorAltas = 0;
        }
        else if(marcador > 0)
        {
            p = buffer1.front();

            buffer1.erase(buffer1.begin());

            marcador--;
            contadorAltas++;
        }
        mtxBuffer1.unlock();

        // se calcula la cantidad de tiempo de espera de cada paquete
        auto actual = chrono::system_clock::now(); //tiempo actual
        auto espera = chrono::duration_cast<chrono::milliseconds>(actual - p.fechaDeCreacion).count();
        if(p.prioridad == 1){
                SumAlta += espera;
                CantAlta++;
                }else
                {
                    SumBaja += espera;
                    cantBaja++;
                }

        p.ingresoProcessing = chrono::system_clock::now(); //momento donde ingresa a la cinta
        // Pasar a la cinta
        mtxBuffer2.lock();
        buffer2.push(p);
        mtxBuffer2.unlock();

        mtxConsola.lock();
        cout << "paquete en cinta "<< p.id<< " prioridad "<< p.prioridad<< endl;
        mtxConsola.unlock();

        //se respetan los 550 ms en el PROCESSING QUEUE
        mtxBuffer2.lock();
        Paquete proc = buffer2.front();
        mtxBuffer2.unlock();

        auto ahora = chrono::system_clock::now();

        auto tiempoEnLaCinta =chrono::duration_cast<chrono::milliseconds>(ahora - proc.ingresoProcessing);

        if (tiempoEnLaCinta.count() < 550)
        {
            this_thread::sleep_for(chrono::milliseconds(550 - tiempoEnLaCinta.count()));
        }

        // Procesarlo inmediatamente

        mtxBuffer2.lock();
        p = buffer2.front();
        buffer2.pop();
        mtxBuffer2.unlock();

        signal(HayEspacioBuffer2);

        this_thread::sleep_for(chrono::milliseconds(270)); //retardo de 270ms entre cada liberación de paquete
        mtxPendientes.lock();
        paquetesPendientes--;
        mtxPendientes.unlock();

        mtxConsola.lock();

        time_t t =chrono::system_clock::to_time_t(p.fechaDeCreacion);

        cout << "Paquete procesado: ";
        cout << "ID: " << p.id<< " Prioridad: " << p.prioridad<< " Fecha: " << ctime(&t);

        mtxConsola.unlock();
    }
}


int main()
{
    init(HayEspacioBuffer2,5);
    init(hayDatosBuffer1,0);
    //init(hayDatosBuffer2,0);

    thread p1(productor);
    thread c1(consumidor);
    thread c2(consumidor);

    p1.join();
    c1.join();
    c2.join();

    std::cout << "Total de paquetes producidos: " << totalProducidos << endl;

    if(CantAlta > 0)
{
    double total1 = SumAlta / CantAlta;
    cout << "Tiempo promedio de espera de paquetes con prioridad ALTA: "<< total1 << " ms" << endl;
}

if(cantBaja > 0)
{
    double total2 = SumBaja / cantBaja;
    cout << "Tiempo promedio de espera de paquetes con prioridad BAJA: " << total2 << " ms" << endl;
}

return 0;
}
//mutex en zonas criticas/ evitar race conditions
//ejecutar los escenarios de pruebas


