package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	proto "example1/proto"

	"google.golang.org/grpc"
)

const (
	// La dirección del servidor Lester   = "10.35.168.59:50051"
	// La dirección del servidor Franklin = "10.35.168.60:50052"
	// La dirección del servidor Trevor   = "10.35.168.61:50053"

	DB1Addr = "db1:51000"
	DB2Addr = "db2:52000"
	DB3Addr = "db3:53000"

	Consumidor1Addr = "consumidor1:54100"
	consumidor2Addr = "consumidor2:51100"
	consumidor3Addr = "consumidor3:52200"

	N = 3
	W = 2
	R = 2
)

type BrokerServer struct {
	proto.UnimplementedBrokerServiceServer

	offers    []*proto.Offer
	producers map[string]bool
	processed map[string]map[string]bool
}

func (s *BrokerServer) sendToNodesQuorum(ctx context.Context, offer *proto.Offer) (int, error) {
	nodes := []string{DB1Addr, DB2Addr, DB3Addr}
	var mu sync.Mutex
	var wg sync.WaitGroup
	ackCount := 0

	// timeout por cada llamada
	timeout := 2 * time.Second

	for _, node := range nodes {
		wg.Add(1)
		go func(nodeAddr string) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			conn, err := grpc.DialContext(cctx, nodeAddr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				log.Printf("No se pudo conectar con el nodo %s: %v", nodeAddr, err)
				return
			}
			defer conn.Close()

			client := proto.NewBrokerServiceClient(conn)
			_, err = client.SendOffer(cctx, offer)
			if err != nil {
				log.Printf("Error al enviar la oferta al nodo %s: %v", nodeAddr, err)
				return
			}

			mu.Lock()
			ackCount++
			mu.Unlock()
		}(node)
	}

	// Esperar a que terminen las llamadas (pero no más de un timeout razonable)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout + 500*time.Millisecond):
		// continue, contaremos los ACKs recibidos hasta ahora
	}

	if ackCount < W {
		return ackCount, fmt.Errorf("no se alcanzó quorum de escritura: %d/%d", ackCount, W)
	}
	return ackCount, nil
}

func (s *BrokerServer) SendOffer(ctx context.Context, offer *proto.Offer) (*proto.Response, error) {

	log.Printf("📦 Oferta recibida: ID=%s | Tienda=%s | Producto=%s | Categoria=%s | Precio=%.2f | Stock=%d | Fecha=%s",
		offer.OfertaId, offer.Tienda, offer.Producto, offer.Categoria, offer.Precio, offer.Stock,
		time.Unix(offer.Fecha, 0).Format("2006-01-02 15:04:05"))

	// Validación
	if offer.OfertaId == "" {
		return nil, fmt.Errorf("offer_id is required")
	}

	// Verificar que el producer esté registrado
	if s.producers == nil || !s.producers[offer.ProductoId] {
		log.Printf("❌ Productor no registrado: %s", offer.ProductoId)
		return nil, fmt.Errorf("producer %s not registered", offer.ProductoId)
	}

	// Idempotencia: descartar duplicados
	if s.processed == nil {
		s.processed = make(map[string]map[string]bool)
	}
	if _, ok := s.processed[offer.ProductoId]; !ok {
		s.processed[offer.ProductoId] = make(map[string]bool)
	}
	if s.processed[offer.ProductoId][offer.OfertaId] {
		log.Printf("⚠️ Oferta duplicada ignorada: %s", offer.OfertaId)
		return &proto.Response{Mensaje: fmt.Sprintf("duplicate offer %s ignored", offer.OfertaId)}, nil
	}

	// Intentar enviar a los nodos y esperar quorum W
	ackCount, err := s.sendToNodesQuorum(ctx, offer)
	if err != nil {
		// No alcanzó quorum: responder error al productor
		log.Printf("⚠️ No se alcanzó quorum al replicar la oferta %s: %v", offer.OfertaId, err)
		return nil, fmt.Errorf("no se pudo almacenar la oferta en quorum: %v", err)
	}

	// Guardar en el broker local también (opcional pero útil para consultas)
	s.offers = append(s.offers, offer)

	// Marcar como procesada (idempotencia)
	s.processed[offer.ProductoId][offer.OfertaId] = true

	response := &proto.Response{
		Mensaje: fmt.Sprintf("Oferta %s recibida y replicada (%d acks)", offer.OfertaId, ackCount),
	}

	log.Printf("✅ Oferta procesada correctamente: %s (%d ACKs)", offer.OfertaId, ackCount)
	return response, nil
}

func (s *BrokerServer) GetHistory(ctx context.Context, req *proto.HistoryRequest) (*proto.HistoryResponse, error) {
	// Implementación de lectura por quorum R: consultar los N nodos y devolver
	// solo las ofertas que aparecen en al menos R respuestas.
	nodes := []string{DB1Addr, DB2Addr, DB3Addr}
	type result struct {
		offers []*proto.Offer
		err    error
	}

	ch := make(chan result, len(nodes))
	timeout := 2 * time.Second

	// Consultar cada nodo en paralelo
	for _, node := range nodes {
		go func(nodeAddr string) {
			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			conn, err := grpc.DialContext(cctx, nodeAddr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				ch <- result{nil, err}
				return
			}
			defer conn.Close()
			client := proto.NewBrokerServiceClient(conn)
			resp, err := client.GetHistory(cctx, req)
			if err != nil {
				ch <- result{nil, err}
				return
			}
			ch <- result{resp.Offers, nil}
		}(node)
	}

	// Recolectar respuestas con un deadline
	collected := 0
	offerCounts := make(map[string]int)
	offerSamples := make(map[string]*proto.Offer)

	deadline := time.After(timeout + 500*time.Millisecond)
	for collected < len(nodes) {
		select {
		case r := <-ch:
			collected++
			if r.err != nil || r.offers == nil {
				continue
			}
			for _, of := range r.offers {
				id := of.OfertaId
				offerCounts[id]++
				// Guardar una muestra para devolver más tarde
				if _, ok := offerSamples[id]; !ok {
					// Hacer una copia ligera
					tmp := *of
					offerSamples[id] = &tmp
				}
			}
		case <-deadline:
			// tiempo agotado, procesar lo que tengamos
			collected = len(nodes)
		}
	}

	// Seleccionar ofertas que alcanzaron quorum R
	var resultOffers []*proto.Offer
	for id, cnt := range offerCounts {
		if cnt >= R {
			if sample, ok := offerSamples[id]; ok {
				resultOffers = append(resultOffers, sample)
			}
		}
	}

	// También podríamos complementar con las ofertas locales del broker si lo deseamos,
	// pero aquí seguimos la regla: lectura válida sólo si al menos R nodos coinciden.

	return &proto.HistoryResponse{Offers: resultOffers}, nil
}

func (s *BrokerServer) RegisterProducer(ctx context.Context, req *proto.RegisterRequest) (*proto.RegisterResponse, error) {
	if req == nil || req.ProducerId == "" {
		return &proto.RegisterResponse{Ok: false, Message: "producer_id required"}, nil
	}
	if s.producers == nil {
		s.producers = make(map[string]bool)
	}
	s.producers[req.ProducerId] = true
	return &proto.RegisterResponse{Ok: true, Message: "registered"}, nil
}

func (s *BrokerServer) SendOfferToNodes(ctx context.Context, offer *proto.Offer) (*proto.Response, error) {
	// Conectar con cada uno de los nodos
	nodes := []string{DB1Addr, DB2Addr, DB3Addr}
	var lastErr error
	for _, node := range nodes {
		conn, err := grpc.DialContext(ctx, node, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Printf("No se pudo conectar con el nodo %s: %v", node, err)
			lastErr = err
			continue
		}

		client := proto.NewBrokerServiceClient(conn)
		_, err = client.SendOffer(ctx, offer)
		// Close the connection immediately after use
		_ = conn.Close()
		if err != nil {
			log.Printf("Error al enviar la oferta al nodo %s: %v", node, err)
			lastErr = err
		}
	}

	if lastErr != nil {
		return &proto.Response{
			Mensaje: fmt.Sprintf("Oferta %s enviada con errores", offer.OfertaId),
		}, nil
	}

	return &proto.Response{
		Mensaje: fmt.Sprintf("Oferta %s enviada a todos los nodos", offer.OfertaId),
	}, nil
}

func main() {

	brokenServer := grpc.NewServer()

	proto.RegisterBrokerServiceServer(brokenServer, &BrokerServer{})

	listener, err := net.Listen("tcp", ":54000")
	if err != nil {
		log.Fatalf("Fallo al escuchar: %v", err)
	}

	log.Printf("Broker escuchando en %v", listener.Addr())
	if err := brokenServer.Serve(listener); err != nil {
		log.Fatalf("Fallo al servir: %v", err)
	}

}
