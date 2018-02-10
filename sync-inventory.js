'use strict';

const config = require( 'config' );
const DABC = require( './dabc' );
const WooCommerceAPI = require( 'woocommerce-api' );
const UntappdClient = require( 'node-untappd' );
const querystring = require( 'querystring' );
const PromisePool = require( 'promise-pool-executor' );
const _ = require( 'lodash' );
const parseLinkHeader = require( 'parse-link-header' );

const WooCommerce = new WooCommerceAPI( {
    url: config.get( 'woocommerce.url' ),
    consumerKey: config.get( 'woocommerce.key' ),
    consumerSecret: config.get( 'woocommerce.secret' ),
    wpAPI: true,
    version: 'wc/v2',
} );

const WordPress = new WooCommerceAPI( {
    url: config.get( 'woocommerce.url' ),
    consumerKey: config.get( 'woocommerce.key' ),
    consumerSecret: config.get( 'woocommerce.secret' ),
    wpAPI: true,
    version: 'wp/v2',
} );

let storeMarkerIdMap = {};

const getProducts = ( page = 1 ) => new Promise( ( resolve, reject ) => {
    WooCommerce.get(
        'products?' + querystring.stringify( { per_page: 100, page } ),
        ( err, res, body ) => {
            if ( err ) {
                reject( err );
            } else {
                try {
                    const json = JSON.parse( body );
                    const links = parseLinkHeader( _.get( res, 'headers.link', '' ) );
                    const response = {
                        products: json,
                    };

                    if ( links.next ) {
                        response.next = () => getProducts( page + 1 );
                    }
    
                    resolve( response );
                } catch ( syntaxError ) {
                    console.error( body );
    
                    reject( syntaxError );
                }
            }
        }
    );
} );

const getStoreMapMarkers = ( page = 1 ) => new Promise( ( resolve, reject ) => {
    WordPress.get(
        'stores?' + querystring.stringify( { per_page: 100, page } ),
        ( err, res, body ) => {
            if ( err ) {
                reject( err );
            } else {
                try {
                    const json = JSON.parse( body );

                    resolve( {
                        results: json,
                        totalPages: Number.parseInt( res.headers['x-wp-totalpages'] ),
                    } );
                } catch ( syntaxError ) {
                    console.error( body );

                    reject( syntaxError );
                }
            }
        }
    );
} );

const getAllStoreMapMarkers = () => {
    return getStoreMapMarkers().then( ( response ) => {
        if ( 1 == response.totalPages ) {
            return response.results;
        }

        const tasks = [ { results: response.results } ];

        for ( let i = 0; i < ( response.totalPages - 1 ); i++ ) {
            tasks.push( getStoreMapMarkers( i + 2 ) );
        }

        return Promise.all( tasks ).then( ( resultSets ) => _.flatMap( resultSets, 'results' ) );
    } );
};

const getStoresBySKU = ( sku ) => new Promise( ( resolve, reject ) => {
    DABC.getBeerInventory( sku, ( err, inventory ) => {
        if ( err ) {
            return reject( err );
        }

        const storeNumbers = inventory.stores.map( ( store ) => Number.parseInt( store.store ) );
        
        resolve( storeNumbers );
    } );
} );

const updateBeerStores = ( beerID, storeNumbers ) => new Promise( ( resolve, reject ) => {
    WordPress.put(
        'product/' + beerID,
        { stores: storeNumbers },
        ( err, res, body ) => {
            if ( err ) {
                reject( err );
            } else {
                try {
                    const json = JSON.parse( body );

                    resolve( json );
                } catch ( syntaxError ) {
                    console.error( body );

                    reject( syntaxError );
                }
            }
        }
    );
} );

const processBeers = ( beers ) => {
    // Create a pool with a concurrency limit
    const pool = new PromisePool.PromisePoolExecutor( {
        frequencyLimit: 10,
        frequencyWindow: 5000,
    } );

    return pool.addEachTask( {
        data: beers,
        generator: ( beer ) => {
            console.log( `Processing ${beer.name}.` );
            
            return getStoresBySKU( beer.sku )
                .then( ( storeNumbers ) => {
                    console.log( `${beer.name} available in stores:`, storeNumbers.join( ', ' ) );
                    return storeNumbers.map( ( number ) => storeMarkerIdMap[ number ] || false ).filter( Boolean )
                 } )
                .then( ( storeMarkerIds ) => updateBeerStores( beer.id, storeMarkerIds ) )
                .then( () => console.log( `${beer.name} stores updated.` ) );
        }
    } ).promise().then( ( results ) => {
        console.log( results.length + ' beers processed.' );
    } );
};

const processProductPage = ( page ) => {
    processBeers( page.products ).then( () => {
        if ( page.next ) {
            page.next().then( processProductPage );
        }
    } );
};

getAllStoreMapMarkers().then( ( markers ) => {
    markers.forEach( ( marker ) => {
        const storeNumber = Number.parseInt( marker.slug.split( '-' ).pop() );

        if ( isNaN( storeNumber ) ) return;

        storeMarkerIdMap[ storeNumber ] = marker.id;
    } );

    getProducts().then( ( page ) => processProductPage( page ) );
} );
