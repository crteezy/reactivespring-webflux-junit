
package com.github.crteezy.reactivespring.controller;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.awt.*;
import java.time.Duration;

@RestController
public class FluxAndMonoController {

    @GetMapping("/flux")
    public Flux<Integer> returnFlux(){
        return Flux.just(1,2,3,4)
                .log();
    }

    @GetMapping(value = "/fluxStream", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
    public Flux<Integer> returnFluxStream(){
        return Flux.just(1,2,3,4)
                .delaySequence(Duration.ofSeconds(1))
                .log();
    }

    @GetMapping(value = "/fluxStreamInfinite", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
    public Flux<Long> returnFluxStreamInfinite(){
        return Flux.interval(Duration.ofSeconds(1))
                .log();
    }
}