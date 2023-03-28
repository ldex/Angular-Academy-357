import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, catchError, delay, shareReplay, tap, first, map, mergeAll, BehaviorSubject, switchMap, of, filter } from 'rxjs';
import { Product } from '../products/product.interface';

@Injectable({
  providedIn: 'root'
})
export class ProductService {

  private baseUrl = 'https://storerestservice.azurewebsites.net/api/products/';
  private productsSubject = new BehaviorSubject<Product[]>([]);
  products$: Observable<Product[]> = this.productsSubject.asObservable();
  mostExpensiveProduct$: Observable<Product>;
  productsToLoad = 10;

  constructor(private http: HttpClient) {
    this.initProducts();
    this.initMostExpensiveProduct();
  }

  private initMostExpensiveProduct() {
    this.mostExpensiveProduct$ =
      this
      .products$
      .pipe(
        filter(products => products.length > 0),
        switchMap(
          products => of(products)
                        .pipe(
                          map(products => [...products].sort((p1, p2) => p1.price > p2.price ? -1 : 1)),
                          // [{p1}, {p2}, {p3}]
                          mergeAll(),
                          // {p1}, {p2}, {p3}
                          first()
                        )
        )
      )
  }

  initProducts(skip = 0, take = this.productsToLoad) {
    let url = this.baseUrl + `?$skip=${skip}&$top=${take}&$orderby=Price%20asc`;

    this
      .http
      .get<Product[]>(url)
      .pipe(
        delay(1500), // For demo...
        tap(console.table),
        shareReplay(),
        map(
          newProducts => {
            let currentProducts = this.productsSubject.value;
            return currentProducts.concat(newProducts);
          }
        )
      )
      .subscribe(
        fullProductList => this.productsSubject.next(fullProductList)
      );
  }

  insertProduct(newProduct: Product): Observable<Product> {
    return this.http.post<Product>(this.baseUrl, newProduct).pipe(delay(2000));
  }

  deleteProduct(id: number): Observable<any> {
    return this.http.delete(this.baseUrl + id);
  }

  resetList() {
    this.productsSubject.next([]);
    this.initProducts();
  }
}
