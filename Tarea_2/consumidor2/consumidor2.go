package main

import (
	"context"
	"fmt"
	"log"

	proto "example1/proto"

	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("broken:54000", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Error al conectar con el broker: %v", err)
	}
	defer conn.Close()

	client := proto.NewBrokerServiceClient(conn)

	consumerID := "C-E1"

	req := &proto.HistoryRequest{
		ConsumidorId: consumerID,
	}
	resp, err := client.GetHistory(context.Background(), req)
	if err != nil {
		log.Fatalf("Error al obtener el historial de ofertas: %v", err)
	}

	fmt.Println("Ofertas recibidas:")
	for _, offer := range resp.Offers {
		fmt.Printf("Oferta ID: %s, Producto: %s, Precio: %.2f\n", offer.OfertaId, offer.Producto, offer.Precio)
	}
}
